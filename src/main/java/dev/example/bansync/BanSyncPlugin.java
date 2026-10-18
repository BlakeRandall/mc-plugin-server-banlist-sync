package dev.example.bansync;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.bson.Document;
import org.bukkit.BanEntry;
import org.bukkit.BanList;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import com.mongodb.client.model.WriteModel;

/**
 * Shared ban synchronization.
 *
 * All Bukkit ban-list reads and writes are performed on the main server thread.
 * MongoDB operations run on a dedicated single worker thread.
 *
 * Player bans use UUIDs as their canonical identity. IP bans use IP addresses.
 * Inactive records are retained as tombstones so unbans propagate between
 * servers.
 */
public final class BanSyncPlugin extends JavaPlugin {

    private record BanRecord(
            String key,
            String type,
            String target,
            String uuid,
            String reason,
            String source,
            Long expiresAt
            ) {

        String signature() {
            return type
                    + "|"
                    + (type.equals("PLAYER") ? uuid : target)
                    + "|"
                    + reason
                    + "|"
                    + source
                    + "|"
                    + expiresAt;
        }
    }

    private final AtomicBoolean cycleRunning = new AtomicBoolean(false);
    private final AtomicLong lastSuccessfulSync = new AtomicLong(0L);

    private volatile Map<String, String> lastAppliedSharedSnapshot;
    private volatile String lastError = "Not connected yet";
    private volatile MongoCollection<Document> collection;

    private MongoClient mongoClient;
    private ExecutorService databaseExecutor;
    private BukkitTask pollTask;

    private String serverId;
    private int intervalSeconds;
    private boolean verbose;
    private String startupMode;
    private boolean indexesReady;
    private int mongoBatchSize;
    private int applyBatchSize;
    private boolean syncPlayers;
    private boolean syncIps;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        loadSettings();

        databaseExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "BanSync-MongoDB");
            thread.setDaemon(true);
            return thread;
        });

        createMongoClient();
        startPolling();

        getLogger().info(
                "BanSync enabled; initial sync will run shortly."
        );
    }

    @Override
    public void onDisable() {
        if (pollTask != null) {
            pollTask.cancel();
        }

        if (databaseExecutor != null) {
            databaseExecutor.shutdownNow();
        }

        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private String loadOrCreateServerId() {
        String serverId = getConfig()
                .getString("server-id", "")
                .trim();

        if (!serverId.isEmpty()) {
            try {
                return UUID.fromString(serverId).toString();
            } catch (IllegalArgumentException exception) {
                getLogger().warning(
                        "Invalid server-id in config.yml; generating a new ID."
                );
            }
        }

        serverId = UUID.randomUUID().toString();

        getConfig().set("server-id", serverId);
        saveConfig();

        getLogger().info("Generated and saved server ID: " + serverId);

        return serverId;
    }

    private void loadSettings() {
        FileConfiguration config = getConfig();

        serverId = loadOrCreateServerId();

        intervalSeconds = Math.max(
                2,
                config.getInt("sync-interval-seconds", 5)
        );

        verbose = config.getBoolean("verbose-logging", true);

        startupMode = config.getString("startup-mode", "merge")
                .trim()
                .toLowerCase(Locale.ROOT);

        mongoBatchSize = Math.max(
                100,
                config.getInt("mongo-batch-size", 1000)
        );

        applyBatchSize = Math.max(
                25,
                config.getInt("apply-batch-size", 200)
        );

        syncPlayers = config.getBoolean("sync-player-bans", true);
        syncIps = config.getBoolean("sync-ip-bans", true);

        if (!startupMode.equals("merge")) {
            getLogger().warning(
                    "Unknown startup-mode '" + startupMode
                    + "'; using merge."
            );

            startupMode = "merge";
        }
    }

    private void createMongoClient() {
        try {
            String uri = getConfig()
                    .getString("mongodb.uri", "")
                    .trim();

            if (uri.isEmpty() || uri.contains("CHANGE_ME")) {
                throw new IllegalArgumentException(
                        "Set mongodb.uri in plugins/BanSync/config.yml first."
                );
            }

            if (mongoClient != null) {
                mongoClient.close();
            }

            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(uri))
                    .applyToSocketSettings(builder
                            -> builder.connectTimeout(
                                    Math.max(
                                            1000,
                                            getConfig().getInt(
                                                    "mongodb.connect-timeout-ms",
                                                    5000
                                            )
                                    ),
                                    TimeUnit.MILLISECONDS
                            )
                    )
                    .applyToClusterSettings(builder
                            -> builder.serverSelectionTimeout(
                                    Math.max(
                                            1000,
                                            getConfig().getInt(
                                                    "mongodb.server-selection-timeout-ms",
                                                    5000
                                            )
                                    ),
                                    TimeUnit.MILLISECONDS
                            )
                    )
                    .build();

            mongoClient = MongoClients.create(settings);

            MongoDatabase database = mongoClient.getDatabase(
                    getConfig().getString(
                            "mongodb.database",
                            "minecraft"
                    )
            );

            collection = database.getCollection(
                    getConfig().getString(
                            "mongodb.collection",
                            "shared_bans"
                    )
            );

            indexesReady = false;
            lastError = null;

        } catch (Exception exception) {
            collection = null;
            lastError = exception.getMessage();

            getLogger().log(
                    Level.SEVERE,
                    "MongoDB configuration error: "
                    + exception.getMessage()
            );
        }
    }

    private void startPolling() {
        if (pollTask != null) {
            pollTask.cancel();
        }

        long periodTicks = Math.max(
                40L,
                intervalSeconds * 20L
        );

        pollTask = Bukkit.getScheduler().runTaskTimer(
                this,
                this::beginSync,
                40L,
                periodTicks
        );
    }

    private void beginSync() {
        if (!cycleRunning.compareAndSet(false, true)) {
            return;
        }

        if (collection == null) {
            createMongoClient();

            if (collection == null) {
                cycleRunning.set(false);
                return;
            }
        }

        Map<String, BanRecord> localSnapshot = currentBans();

        Map<String, String> baseline = lastAppliedSharedSnapshot == null
                ? null
                : new HashMap<>(lastAppliedSharedSnapshot);

        databaseExecutor.execute(() -> {
            try {
                MongoCollection<Document> coll = collection;

                if (coll == null) {
                    throw new IllegalStateException(
                            "MongoDB collection is unavailable"
                    );
                }

                if (!indexesReady) {
                    coll.createIndex(
                            Indexes.ascending("key"),
                            new IndexOptions()
                                    .unique(true)
                                    .name("unique_ban_key")
                    );

                    coll.createIndex(
                            Indexes.ascending("active"),
                            new IndexOptions().name("active_lookup")
                    );

                    indexesReady = true;
                }

                coll.find().limit(1).first();

                if (baseline == null && startupMode.equals("merge")) {
                    seedLocalBans(coll, localSnapshot);

                } else if (baseline != null) {
                    Set<String> keys = new HashSet<>(baseline.keySet());
                    keys.addAll(localSnapshot.keySet());

                    for (String key : keys) {
                        String previousSignature = baseline.get(key);

                        BanRecord localRecord = localSnapshot.get(key);

                        String currentSignature = localRecord == null
                                ? null
                                : localRecord.signature();

                        if (Objects.equals(
                                previousSignature,
                                currentSignature
                        )) {
                            continue;
                        }

                        if (localRecord == null) {
                            publishTombstone(
                                    coll,
                                    key,
                                    "local-change"
                            );
                        } else {
                            publish(
                                    coll,
                                    localRecord,
                                    true,
                                    "local-change"
                            );
                        }
                    }
                }

                Map<String, BanRecord> sharedActive = fetchActiveBans(coll);

                Bukkit.getScheduler().runTask(this, ()
                        -> applySharedBans(sharedActive, () -> {
                            Map<String, String> applied = new HashMap<>();

                    sharedActive.forEach((key, record)
                            -> applied.put(
                                            key,
                                            record.signature()
                                    )
                            );

                            lastAppliedSharedSnapshot = applied;
                            lastSuccessfulSync.set(
                                    System.currentTimeMillis()
                            );

                            lastError = null;
                            cycleRunning.set(false);
                        })
                );

            } catch (Exception exception) {
                lastError = exception.getMessage();

                getLogger().log(
                        Level.WARNING,
                        "MongoDB sync failed; local bans were not "
                        + "reconciled. Will retry. Cause: "
                        + exception.getMessage()
                );

                cycleRunning.set(false);
            }
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Map<String, BanRecord> currentBans() {
        Map<String, BanRecord> result = new HashMap<>();

        if (syncPlayers) {
            BanList list = Bukkit.getBanList(BanList.Type.NAME);

            for (Object object : list.getBanEntries()) {
                BanEntry entry = (BanEntry) object;

                String target = String.valueOf(entry.getTarget());
                UUID uuid;

                try {
                    uuid = UUID.fromString(target);
                } catch (IllegalArgumentException exception) {
                    OfflinePlayer player = Bukkit.getOfflinePlayer(target);
                    uuid = player.getUniqueId();
                }

                String key = "PLAYER:" + uuid;

                result.put(
                        key,
                        new BanRecord(
                                key,
                                "PLAYER",
                                target,
                                uuid.toString(),
                                nullToEmpty(entry.getReason()),
                                nullToEmpty(entry.getSource()),
                                millis(entry.getExpiration())
                        )
                );
            }
        }

        if (syncIps) {
            BanList list = Bukkit.getBanList(BanList.Type.IP);

            for (Object object : list.getBanEntries()) {
                BanEntry entry = (BanEntry) object;

                String target = String.valueOf(entry.getTarget());
                String key = "IP:" + target;

                result.put(
                        key,
                        new BanRecord(
                                key,
                                "IP",
                                target,
                                "",
                                nullToEmpty(entry.getReason()),
                                nullToEmpty(entry.getSource()),
                                millis(entry.getExpiration())
                        )
                );
            }
        }

        return result;
    }

    private void publish(
            MongoCollection<Document> coll,
            BanRecord record,
            boolean active,
            String source
    ) {
        coll.updateOne(
                eq("key", record.key()),
                combine(
                        set("type", record.type()),
                        set("target", record.target()),
                        set("uuid", record.uuid()),
                        set("reason", record.reason()),
                        set("source", record.source()),
                        set(
                                "expiresAt",
                                record.expiresAt() == null
                                ? null
                                : new Date(record.expiresAt())
                        ),
                        set("active", active),
                        set("changedBy", serverId),
                        set("changeSource", source),
                        currentDate("updatedAt"),
                        inc("revision", 1L)
                ),
                new UpdateOptions().upsert(true)
        );

        if (verbose) {
            getLogger().info(
                    "Published "
                    + (active ? "ADD " : "REMOVE ")
                    + record.key()
                    + " to shared bans."
            );
        }
    }

    private void publishTombstone(
            MongoCollection<Document> coll,
            String key,
            String source
    ) {
        coll.updateOne(
                eq("key", key),
                combine(
                        set("active", false),
                        set("changedBy", serverId),
                        set("changeSource", source),
                        currentDate("updatedAt"),
                        inc("revision", 1L)
                ),
                new UpdateOptions().upsert(true)
        );

        if (verbose) {
            getLogger().info(
                    "Published REMOVE " + key + " to shared bans."
            );
        }
    }

    private Map<String, BanRecord> fetchActiveBans(
            MongoCollection<Document> coll
    ) {
        Map<String, BanRecord> active = new HashMap<>();
        long now = System.currentTimeMillis();

        for (Document document : coll.find(eq("active", true))
                .batchSize(mongoBatchSize)) {

            String key = document.getString("key");
            String type = document.getString("type");
            String target = document.getString("target");

            if (key == null || type == null || target == null) {
                continue;
            }

            if (!type.equals("PLAYER") && !type.equals("IP")) {
                getLogger().warning(
                        "Ignoring unknown ban type in MongoDB: "
                        + type + " (" + key + ")"
                );
                continue;
            }

            if (type.equals("PLAYER") && !syncPlayers) {
                continue;
            }

            if (type.equals("IP") && !syncIps) {
                continue;
            }

            Date expiration = document.getDate("expiresAt");

            if (expiration != null && expiration.getTime() <= now) {
                publishTombstone(coll, key, "expired");
                continue;
            }

            String uuid = nullToEmpty(document.getString("uuid"));

            if (type.equals("PLAYER")) {
                try {
                    UUID parsedUuid = UUID.fromString(uuid);

                    if (!key.equals("PLAYER:" + parsedUuid)) {
                        getLogger().warning(
                                "Ignoring inconsistent player-ban key: "
                                + key
                        );
                        continue;
                    }
                } catch (IllegalArgumentException exception) {
                    getLogger().warning(
                            "Ignoring malformed player-ban UUID: " + key
                    );
                    continue;
                }
            }

            active.put(
                    key,
                    new BanRecord(
                            key,
                            type,
                            target,
                            uuid,
                            nullToEmpty(document.getString("reason")),
                            nullToEmpty(document.getString("source")),
                            millis(expiration)
                    )
            );
        }

        return active;
    }

    private void applySharedBans(
            Map<String, BanRecord> shared,
            Runnable onSuccess
    ) {
        Map<String, BanRecord> local = currentBans();

        List<BanRecord> toAddOrUpdate = new ArrayList<>();
        List<String> toRemove = new ArrayList<>();

        for (Map.Entry<String, BanRecord> entry : shared.entrySet()) {
            String key = entry.getKey();
            BanRecord sharedRecord = entry.getValue();
            BanRecord localRecord = local.get(key);

            if (localRecord == null
                    || !Objects.equals(
                            localRecord.signature(),
                            sharedRecord.signature()
                    )) {
                toAddOrUpdate.add(sharedRecord);
            }
        }

        for (String key : local.keySet()) {
            if (!shared.containsKey(key)) {
                toRemove.add(key);
            }
        }

        List<Runnable> operations = new ArrayList<>(
                toAddOrUpdate.size() + toRemove.size()
        );

        for (BanRecord record : toAddOrUpdate) {
            operations.add(() -> applyBan(record));
        }

        for (String key : toRemove) {
            operations.add(() -> removeBan(key));
        }

        applyBatch(operations, 0, onSuccess);

        getLogger().info(
                "Ban reconciliation queued: add/update="
                + toAddOrUpdate.size()
                + ", remove="
                + toRemove.size()
        );
    }

    private void applyBan(BanRecord record) {
        Date expiration = record.expiresAt() == null
                ? null
                : new Date(record.expiresAt());

        if (record.type().equals("PLAYER") && syncPlayers) {
            UUID uuid = UUID.fromString(record.uuid());

            OfflinePlayer player = Bukkit.getOfflinePlayer(uuid);

            String name = player.getName();

            if (name == null || name.isBlank()) {
                name = record.target();
            }

            BanList list = Bukkit.getBanList(BanList.Type.NAME);

            list.pardon(name);
            list.pardon(uuid.toString());

            list.addBan(
                    name,
                    emptyToNull(record.reason()),
                    expiration,
                    emptyToNull(record.source())
            );

        } else if (record.type().equals("IP") && syncIps) {
            BanList list = Bukkit.getBanList(BanList.Type.IP);

            list.pardon(record.target());

            list.addBan(
                    record.target(),
                    emptyToNull(record.reason()),
                    expiration,
                    emptyToNull(record.source())
            );
        }
    }

    private void removeBan(String key) {
        if (key.startsWith("PLAYER:") && syncPlayers) {
            String uuid = key.substring("PLAYER:".length());
            String name = uuid;

            try {
                OfflinePlayer player = Bukkit.getOfflinePlayer(
                        UUID.fromString(uuid)
                );

                if (player.getName() != null
                        && !player.getName().isBlank()) {
                    name = player.getName();
                }
            } catch (IllegalArgumentException exception) {
                getLogger().warning(
                        "Cannot remove player ban with malformed UUID: "
                        + key
                );
                return;
            }

            BanList list = Bukkit.getBanList(BanList.Type.NAME);

            list.pardon(name);
            list.pardon(uuid);

        } else if (key.startsWith("IP:") && syncIps) {
            String ip = key.substring("IP:".length());

            Bukkit.getBanList(BanList.Type.IP).pardon(ip);
        }
    }

    private void applyBatch(
            List<Runnable> operations,
            int offset,
            Runnable onSuccess
    ) {
        try {
            int end = Math.min(
                    offset + applyBatchSize,
                    operations.size()
            );

            for (int i = offset; i < end; i++) {
                operations.get(i).run();
            }

            if (end < operations.size()) {
                Bukkit.getScheduler().runTask(
                        this,
                        () -> applyBatch(
                                operations,
                                end,
                                onSuccess
                        )
                );
            } else {
                onSuccess.run();
            }

        } catch (Exception exception) {
            lastError = exception.getMessage();
            cycleRunning.set(false);

            getLogger().log(
                    Level.WARNING,
                    "Ban batch failed; next sync will retry.",
                    exception
            );
        }
    }

    private void seedLocalBans(
            MongoCollection<Document> coll,
            Map<String, BanRecord> localSnapshot
    ) {
        List<WriteModel<Document>> batch = new ArrayList<>(mongoBatchSize);

        for (BanRecord record : localSnapshot.values()) {
            Document seed = new Document("key", record.key())
                    .append("type", record.type())
                    .append("target", record.target())
                    .append("uuid", record.uuid())
                    .append("reason", record.reason())
                    .append("source", record.source())
                    .append(
                            "expiresAt",
                            record.expiresAt() == null
                            ? null
                            : new Date(record.expiresAt())
                    )
                    .append("active", true)
                    .append("changedBy", serverId)
                    .append("changeSource", "startup-merge")
                    .append("updatedAt", new Date())
                    .append("revision", 1L);

            batch.add(new UpdateOneModel<>(
                    eq("key", record.key()),
                    new Document("$setOnInsert", seed),
                    new UpdateOptions().upsert(true)
            ));

            if (batch.size() >= mongoBatchSize) {
                coll.bulkWrite(
                        batch,
                        new BulkWriteOptions().ordered(false)
                );

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            coll.bulkWrite(
                    batch,
                    new BulkWriteOptions().ordered(false)
            );
        }

        getLogger().info(
                "Seeded local ban candidates: " + localSnapshot.size()
        );
    }

    private static Long millis(Date date) {
        return date == null ? null : date.getTime();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    public String statusText() {
        long last = lastSuccessfulSync.get();

        String age = last == 0L
                ? "never"
                : ((System.currentTimeMillis() - last) / 1000L)
                + "s ago";

        return "last-success="
                + age
                + ", interval="
                + intervalSeconds
                + "s, "
                + (lastError == null
                        ? "database=ok"
                        : "database/error=" + lastError);
    }

    @Override
    public boolean onCommand(
            CommandSender sender,
            Command command,
            String label,
            String[] args
    ) {
        if (!command.getName().equalsIgnoreCase("bansync")) {
            return false;
        }

        if (!sender.hasPermission("bansync.admin")) {
            sender.sendMessage(
                    "You do not have permission to use this command."
            );
            return true;
        }

        String subcommand = args.length == 0
                ? "status"
                : args[0].toLowerCase(Locale.ROOT);

        switch (subcommand) {
            case "status" ->
                sender.sendMessage(
                        "BanSync: " + statusText()
                );

            case "sync" -> {
                if (cycleRunning.get()) {
                    sender.sendMessage(
                            "BanSync: sync already running."
                    );
                } else {
                    Bukkit.getScheduler().runTask(
                            this,
                            this::beginSync
                    );

                    sender.sendMessage("BanSync: sync queued.");
                }
            }

            case "reload" -> {
                if (cycleRunning.get()) {
                    sender.sendMessage(
                            "BanSync: wait for the current sync "
                            + "to finish before reloading."
                    );
                    return true;
                }

                reloadConfig();
                loadSettings();
                createMongoClient();
                startPolling();

                sender.sendMessage(
                        "BanSync: configuration reloaded; "
                        + "database configuration valid: "
                        + (collection != null)
                );
            }

            default ->
                sender.sendMessage(
                        "Usage: /bansync <sync|status|reload>"
                );
        }

        return true;
    }
}
