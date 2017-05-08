#! This is a library to interface with
#! [Gluster](https:#gluster.readthedocs.org/en/latest/)
#!
#! Most of the commands below are wrappers around the CLI functionality.
#! However recently I have
#! reverse engineered some of the Gluster RPC protocol so that calls can be
#! made directly against
#! the Gluster server.  This method of communication is much faster than
#! shelling out.
#!
#! Scale testing with this library has been done to about 60 servers
#! successfully.
#!
#! Please file any bugs found at: [Gluster
#! Repo](https:#github.com/cholcombe973/Gluster)
#! Pull requests are more than welcome!

from enum import Enum

import re
from volume import Brick, volume_info

class SelfHealAlgorithm(Enum):
    Full = "full"
    Diff = "diff"
    Reset = "reset"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "full":
            return SelfHealAlgorithm.Full
        elif s== "diff":
            return SelfHealAlgorithm.Diff
        elif s == "reset":
            return SelfHealAlgorithm.Reset
        else:
            return None

class SplitBrainPolicy(Enum):
    Ctime = "ctime"
    Disable = "none"
    Majority = "majority"
    Mtime = "mtime"
    Size = "size"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "ctime":
            return SplitBrainPolicy.Ctime
        elif s == "none":
            return SplitBrainPolicy.Disable
        elif s == "majority":
            return SplitBrainPolicy.Majority
        elif s == "mtime":
            return SplitBrainPolicy.Mtime
        elif s == "size":
            return SplitBrainPolicy.Size,
        else:
            return None

class AccessMode(Enum):
    ReadOnly = "read-only"
    ReadWrite = "read-write"
    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "read-only":
            return AccessMode.ReadOnly
        elif s == "read-write":
            return AccessMode.ReadWrite
        else:
            return None

class Toggle(Enum):
    On = True
    Off = False

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if  s == "on":
            return Toggle.On
        elif s == "off":
            return Toggle.Off
        elif s == "True":
            return Toggle.On
        elif s == "False":
            return Toggle.Off
        else:
            return None

class ScrubSchedule(Enum):
    Hourly = "hourly"
    Daily = "daily"
    Weekly = "weekly"
    BiWeekly = "biweekly"
    Monthly = "monthly"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "hourly":
            return ScrubSchedule.Hourly
        elif s == "daily":
            return ScrubSchedule.Daily
        elif s == "weekly":
            return ScrubSchedule.Weekly
        elif s == "biweekly":
            return ScrubSchedule.BiWeekly
        elif s == "monthly":
            return ScrubSchedule.Monthly
        else:
            return None

class ScrubAggression(Enum):
    Aggressive = "aggressive"
    Lazy = "lazy"
    Normal = "normal"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "aggressive":
            return ScrubAggression.Aggressive
        elif s == "lazy":
            return ScrubAggression.Lazy
        elif s == "normal":
            return ScrubAggression.Normal
        else:
            return None

class ScrubControl(Enum):
    Pause = "pause"
    Resume = "resume"
    Status = "status"
    OnDemand = "ondemand"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "pause":
            return ScrubControl.Pause
        elif s == "resume":
            return ScrubControl.Resume
        elif s == "status":
            return ScrubControl.Status
        elif s == "ondemand":
            return ScrubControl.OnDemand
        else:
            return None

class BitrotOption(object):
    ScrubThrottle = ScrubAggression
    ScrubFrequency = ScrubSchedule
    Scrub = ScrubControl

    def __str__(self):
        return "{}".format(self.value)

    def to_string(self) -> String
        match self
            BitrotOption::ScrubThrottle(_) => "scrub-throttle",
            BitrotOption::ScrubFrequency(_) => "scrub-frequency",
            BitrotOption::Scrub(_) => "scrub",

    def value(self) -> String
        match self
            BitrotOption::ScrubThrottle(ref val) => val,
            BitrotOption::ScrubFrequency(ref val) => val,
            BitrotOption::Scrub(ref val) => val,

class GlusterOption(Enum):
    # Valid IP address which includes wild card patterns including *, such as 192.168.1.*
    AuthAllow(String),
    # Valid IP address which includes wild card patterns including *, such as 192.168.2.*
    AuthReject(String),
    # Specifies the duration for the lock state to be maintained on the client after a
    # network disconnection in seconds
    # Range: 10-1800
    ClientGraceTimeout(i64),
    # Specifies the maximum number of blocks per file on which self-heal would happen
    # simultaneously.
    # Range: 0-1025
    ClusterSelfHealWindowSize(u16),
    # enable/disable client.ssl flag in the volume
    ClientSsl(Toggle),
    # Specifies the type of self-heal. If you set the option as "full", the entire file is
    # copied from source to destinations. If the option is set to "diff" the file blocks
    # that are not in sync are copied to destinations.
    ClusterDataSelfHealAlgorithm(SelfHealAlgorithm),
    # Percentage of required minimum free disk space
    ClusterMinFreeDisk(u8),
    # Specifies the size of the stripe unit that will be read from or written to in bytes
    ClusterStripeBlockSize(u64),
    # Allows you to turn-off proactive self-heal on replicated
    ClusterSelfHealDaemon(Toggle),
    # This option makes sure the data/metadata is durable across abrupt shutdown of the brick.
    ClusterEnsureDurability(Toggle),
    # The log-level of the bricks.
    DiagnosticsBrickLogLevel(log::LogLevel),
    # The log-level of the clients.
    DiagnosticsClientLogLevel(log::LogLevel),
    # Interval in which we want to collect FOP latency samples.  2 means collect a sample every
    # 2nd FOP.
    DiagnosticsFopSampleInterval(u64),
    # The maximum size of our FOP sampling ring buffer. Default: 65535
    DiagnosticsFopSampleBufSize(u64),
    # Enable the File Operation count translator
    DiagnosticsCountFopHits(Toggle),
    # Interval (in seconds) at which to auto-dump statistics. Zero disables automatic dumping.
    DiagnosticsStatsDumpInterval(u64),
    # The interval after wish a cached DNS entry will be re-validated.  Default: 24 hrs
    DiagnosticsStatsDnscacheTtlSec(u64),
    # Statistics related to the latency of each operation would be tracked.
    DiagnosticsLatencyMeasurement(Toggle),
    # Statistics related to file-operations would be tracked.
    DiagnosticsDumpFdStats(Toggle),
    # Enables automatic resolution of split brain issues
    FavoriteChildPolicy(SplitBrainPolicy),
    # Enables you to mount the entire volume as read-only for all the clients
    # (including NFS clients) accessing it.
    FeaturesReadOnly(Toggle),
    # Enables self-healing of locks when the network disconnects.
    FeaturesLockHeal(Toggle),
    # For performance reasons, quota caches the directory sizes on client. You can set timeout
    # indicating the maximum duration of directory sizes in cache, from the time they are
    # populated, during which they are considered valid
    FeaturesQuotaTimeout(u16),
    # Automatically sync the changes in the filesystem from Master to Slave.
    GeoReplicationIndexing(Toggle),
    # The time frame after which the operation has to be declared as dead, if the server does
    # not respond for a particular operation.
    NetworkFrameTimeout(u16),
    # For 32-bit nfs clients or applications that do not support 64-bit inode numbers or large
    # files, use this option from the CLI to make Gluster NFS return 32-bit inode numbers
    # instead of 64-bit inode numbers.
    NfsEnableIno32(Toggle),
    # Set the access type for the specified sub-volume.
    NfsVolumeAccess(AccessMode),
    # If there is an UNSTABLE write from the client, STABLE flag will be returned to force the
    # client to not send a COMMIT request. In some environments, combined with a replicated
    # GlusterFS setup, this option can improve write performance. This flag allows users to
    # trust Gluster replication logic to sync data to the disks and recover when required.
    # COMMIT requests if received will be handled in a default manner by fsyncing. STABLE writes
    # are still handled in a sync manner.
    NfsTrustedWrite(Toggle),
    # All writes and COMMIT requests are treated as async. This implies that no write requests
    # are guaranteed to be on server disks when the write reply is received at the NFS client.
    # Trusted sync includes trusted-write behavior.
    NfsTrustedSync(Toggle),
    # This option can be used to export specified comma separated subdirectories in the volume.
    # The path must be an absolute path. Along with path allowed list of IPs/hostname can be
    # associated with each subdirectory. If provided connection will allowed only from these IPs.
    # Format: \<dir>[(hostspec[hostspec...])][,...]. Where hostspec can be an IP address,
    # hostname or an IP range in CIDR notation. Note: Care must be taken while configuring
    # this option as invalid entries and/or unreachable DNS servers can introduce unwanted
    # delay in all the mount calls.
    NfsExportDir(String),
    # Enable/Disable exporting entire volumes, instead if used in conjunction with
    # nfs3.export-dir, can allow setting up only subdirectories as exports.
    NfsExportVolumes(Toggle),
    # Enable/Disable the AUTH_UNIX authentication type. This option is enabled by default for
    # better interoperability. However, you can disable it if required.
    NfsRpcAuthUnix(Toggle),
    # Enable/Disable the AUTH_NULL authentication type. It is not recommended to change
    # the default value for this option.
    NfsRpcAuthNull(Toggle),
    # Allow client connections from unprivileged ports. By default only privileged ports are
    # allowed. This is a global setting in case insecure ports are to be enabled for all
    # exports using a single option.
    NfsPortsInsecure(Toggle),
    # Turn-off name lookup for incoming client connections using this option. In some setups,
    # the name server can take too long to reply to DNS queries resulting in timeouts of mount
    # requests. Use this option to turn off name lookups during address authentication. Note,
    NfsAddrNamelookup(Toggle),
    # For systems that need to run multiple NFS servers, you need to prevent more than one from
    # registering with portmap service. Use this option to turn off portmap registration for
    # Gluster NFS.
    NfsRegisterWithPortmap(Toggle),
    # Turn-off volume being exported by NFS
    NfsDisable(Toggle),
    # Size of the per-file write-behind buffer.Size of the per-file write-behind buffer.
    PerformanceWriteBehindWindowSize(u64),
    # The number of threads in IO threads translator.
    PerformanceIoThreadCount(u8),
    # If this option is set ON, instructs write-behind translator to perform flush in
    # background, by returning success (or any errors, if any of previous writes were failed)
    # to application even before flush is sent to backend filesystem.
    PerformanceFlushBehind(Toggle),
    # Sets the maximum file size cached by the io-cache translator. Can use the normal size
    # descriptors of KB, MB, GB,TB or PB (for example, 6GB). Maximum size u64.
    PerformanceCacheMaxFileSize(u64),
    # Sets the minimum file size cached by the io-cache translator. Values same as "max" above
    PerformanceCacheMinFileSize(u64),
    # The cached data for a file will be retained till 'cache-refresh-timeout' seconds,
    # after which data re-validation is performed.
    PerformanceCacheRefreshTimeout(u8),
    # Size of the read cache in bytes
    PerformanceCacheSize(u64),
    # enable/disable readdir-ahead translator in the volume
    PerformanceReadDirAhead(Toggle),
    # If this option is enabled, the readdir operation is performed parallely on all the bricks,
    # thus improving the performance of readdir. Note that the performance improvement is higher
    # in large clusters
    PerformanceParallelReadDir(Toggle),
    # maximum size of cache consumed by readdir-ahead xlator. This value is global and total
    # memory consumption by readdir-ahead is capped by this value, irrespective of the
    # number/size of directories cached
    PerformanceReadDirAheadCacheLimit(u64),
    # Allow client connections from unprivileged ports. By default only privileged ports are
    # allowed. This is a global setting in case insecure ports are to be enabled for all
    # exports using a single option.
    ServerAllowInsecure(Toggle),
    # Specifies the duration for the lock state to be maintained on the server after a
    # network disconnection.
    ServerGraceTimeout(u16),
    # enable/disable server.ssl flag in the volume
    ServerSsl(Toggle),
    # Location of the state dump file.
    ServerStatedumpPath(PathBuf),
    SslAllow(String),
    SslCertificateDepth(u8),
    SslCipherList(String),
    # Number of seconds between health-checks done on the filesystem that is used for the
    # brick(s). Defaults to 30 seconds, set to 0 to disable.
    StorageHealthCheckInterval(u16),

    def to_string(self):
        match self
            GlusterOption::AuthAllow(_) => "auth.allow",
            GlusterOption::AuthReject(_) => "auth.reject",
            GlusterOption::ClientGraceTimeout(_) => "client.grace-timeout",
            GlusterOption::ClientSsl(_) => "client.ssl",
            GlusterOption::ClusterSelfHealWindowSize(_) =>
                "cluster.self-heal-window-size"

            GlusterOption::ClusterDataSelfHealAlgorithm(_) =>
                "cluster.data-self-heal-algorithm"

            GlusterOption::ClusterMinFreeDisk(_) => "cluster.min-free-disk",
            GlusterOption::ClusterStripeBlockSize(_) => "cluster.stripe-block-size",
            GlusterOption::ClusterSelfHealDaemon(_) => "cluster.self-heal-daemon",
            GlusterOption::ClusterEnsureDurability(_) => "cluster.ensure-durability",
            GlusterOption::DiagnosticsBrickLogLevel(_) =>
                "diagnostics.brick-log-level"

            GlusterOption::DiagnosticsClientLogLevel(_) =>
                "diagnostics.client-log-level"

            GlusterOption::DiagnosticsLatencyMeasurement(_) =>
                "diagnostics.latency-measurement"

            GlusterOption::DiagnosticsCountFopHits(_) => "diagnostics.count-fop-hits",
            GlusterOption::DiagnosticsDumpFdStats(_) => "diagnostics.dump-fd-stats",
            GlusterOption::DiagnosticsFopSampleBufSize(_) =>
                "diagnostics.fop-sample-buf-size"

            GlusterOption::DiagnosticsFopSampleInterval(_) =>
                "diagnostics.fop-sample-interval"

            GlusterOption::DiagnosticsStatsDnscacheTtlSec(_) =>
                "diagnostics.stats-dnscache-ttl-sec"

            GlusterOption::DiagnosticsStatsDumpInterval(_) =>
                "diagnostics.stats-dump-interval"

            GlusterOption::FavoriteChildPolicy(_) => "cluster.favorite-child-policy",
            GlusterOption::FeaturesReadOnly(_) => "features.read-only",
            GlusterOption::FeaturesLockHeal(_) => "features.lock-heal",
            GlusterOption::FeaturesQuotaTimeout(_) => "features.quota-timeout",
            GlusterOption::GeoReplicationIndexing(_) => "geo-replication.indexing",
            GlusterOption::NetworkFrameTimeout(_) => "network.frame-timeout",
            GlusterOption::NfsEnableIno32(_) => "nfs.enable-ino32",
            GlusterOption::NfsVolumeAccess(_) => "nfs.volume-access",
            GlusterOption::NfsTrustedWrite(_) => "nfs.trusted-write",
            GlusterOption::NfsTrustedSync(_) => "nfs.trusted-sync",
            GlusterOption::NfsExportDir(_) => "nfs.export-dir",
            GlusterOption::NfsExportVolumes(_) => "nfs.export-volumes",
            GlusterOption::NfsRpcAuthUnix(_) => "nfs.rpc-auth-unix",
            GlusterOption::NfsRpcAuthNull(_) => "nfs.rpc-auth-null",
            GlusterOption::NfsPortsInsecure(_) => "nfs.ports-insecure",
            GlusterOption::NfsAddrNamelookup(_) => "nfs.addr-namelookup",
            GlusterOption::NfsRegisterWithPortmap(_) => "nfs.register-with-portmap",
            GlusterOption::NfsDisable(_) => "nfs.disable",
            GlusterOption::PerformanceWriteBehindWindowSize(_) =>
                "performance.write-behind-window-size"

            GlusterOption::PerformanceIoThreadCount(_) =>
                "performance.io-thread-count"

            GlusterOption::PerformanceFlushBehind(_) => "performance.flush-behind",
            GlusterOption::PerformanceCacheMaxFileSize(_) =>
                "performance.cache-max-file-size"

            GlusterOption::PerformanceCacheMinFileSize(_) =>
                "performance.cache-min-file-size"

            GlusterOption::PerformanceCacheRefreshTimeout(_) =>
                "performance.cache-refresh-timeout"

            GlusterOption::PerformanceCacheSize(_) => "performance.cache-size",
            GlusterOption::PerformanceReadDirAhead(_) => "performance.readdir-ahead",
            GlusterOption::PerformanceParallelReadDir(_) =>
                "performance.parallel-readdir"

            GlusterOption::PerformanceReadDirAheadCacheLimit(_) =>
                "performance.rda-cache-limit"

            GlusterOption::ServerAllowInsecure(_) => "server.allow-insecure",
            GlusterOption::ServerGraceTimeout(_) => "server.grace-timeout",
            GlusterOption::ServerSsl(_) => "server.ssl",
            GlusterOption::ServerStatedumpPath(_) => "server.statedump-path",
            GlusterOption::SslAllow(_) => "auth.ssl-allow",
            GlusterOption::SslCertificateDepth(_) => "ssl.certificate-depth",
            GlusterOption::SslCipherList(_) => "ssl.cipher-list",
            GlusterOption::StorageHealthCheckInterval(_) =>
                "storage.health-check-interval"

    def value(self) -> String
        match self
            GlusterOption::AuthAllow(ref val) => val,
            GlusterOption::AuthReject(ref val) => val,
            GlusterOption::ClientGraceTimeout(val) => val,
            GlusterOption::ClientSsl(ref val) => val,
            GlusterOption::ClusterSelfHealWindowSize(val) => val,
            GlusterOption::ClusterDataSelfHealAlgorithm(ref val) => val,
            GlusterOption::ClusterMinFreeDisk(val) => val,
            GlusterOption::ClusterStripeBlockSize(val) => val,
            GlusterOption::ClusterSelfHealDaemon(ref val) => val,
            GlusterOption::ClusterEnsureDurability(ref val) => val,
            GlusterOption::DiagnosticsBrickLogLevel(val) => val,
            GlusterOption::DiagnosticsClientLogLevel(val) => val,
            GlusterOption::DiagnosticsLatencyMeasurement(ref val) => val,
            GlusterOption::DiagnosticsDumpFdStats(ref val) => val,
            GlusterOption::DiagnosticsFopSampleInterval(ref val) => val,
            GlusterOption::DiagnosticsFopSampleBufSize(ref val) => val,
            GlusterOption::DiagnosticsCountFopHits(ref val) => val,
            GlusterOption::DiagnosticsStatsDumpInterval(ref val) => val,
            GlusterOption::DiagnosticsStatsDnscacheTtlSec(ref val) => val,
            GlusterOption::FavoriteChildPolicy(ref val) => val,
            GlusterOption::FeaturesReadOnly(ref val) => val,
            GlusterOption::FeaturesLockHeal(ref val) => val,
            GlusterOption::FeaturesQuotaTimeout(val) => val,
            GlusterOption::GeoReplicationIndexing(ref val) => val,
            GlusterOption::NetworkFrameTimeout(val) => val,
            GlusterOption::NfsEnableIno32(ref val) => val,
            GlusterOption::NfsVolumeAccess(ref val) => val,
            GlusterOption::NfsTrustedWrite(ref val) => val,
            GlusterOption::NfsTrustedSync(ref val) => val,
            GlusterOption::NfsExportDir(ref val) => val,
            GlusterOption::NfsExportVolumes(ref val) => val,
            GlusterOption::NfsRpcAuthUnix(ref val) => val,
            GlusterOption::NfsRpcAuthNull(ref val) => val,
            GlusterOption::NfsPortsInsecure(ref val) => val,
            GlusterOption::NfsAddrNamelookup(ref val) => val,
            GlusterOption::NfsRegisterWithPortmap(ref val) => val,
            GlusterOption::NfsDisable(ref val) => val,
            GlusterOption::PerformanceWriteBehindWindowSize(val) => val,
            GlusterOption::PerformanceIoThreadCount(val) => val,
            GlusterOption::PerformanceFlushBehind(ref val) => val,
            GlusterOption::PerformanceCacheMaxFileSize(val) => val,
            GlusterOption::PerformanceCacheMinFileSize(val) => val,
            GlusterOption::PerformanceCacheRefreshTimeout(val) => val,
            GlusterOption::PerformanceCacheSize(val) => val,
            GlusterOption::PerformanceReadDirAhead(ref val) => val,
            GlusterOption::PerformanceParallelReadDir(ref val) => val,
            GlusterOption::PerformanceReadDirAheadCacheLimit(val) => val,
            GlusterOption::ServerAllowInsecure(ref val) => val,
            GlusterOption::ServerGraceTimeout(val) => val,
            GlusterOption::ServerSsl(ref val) => val,
            GlusterOption::SslAllow(ref val) => val,
            GlusterOption::SslCertificateDepth(val) => val,
            GlusterOption::SslCipherList(ref val) => val,
            GlusterOption::ServerStatedumpPath(ref val) => val.to_string_lossy().into_owned(),
            GlusterOption::StorageHealthCheckInterval(val) => val,


    def from_str(s, value):
        match s
            "auth-allow" =>
                return Ok(GlusterOption::AuthAllow(value))

            "auth-reject" =>
                return Ok(GlusterOption::AuthReject(value))

            "auth.ssl-allow" =>
                return Ok(GlusterOption::SslAllow(value))

            "client.ssl" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::ClientSsl(t))

            "cluster.favorite-child-policy" =>
                policy = SplitBrainPolicy::from_str(value)?
                return Ok(GlusterOption::FavoriteChildPolicy(policy))

            "client-grace-timeout" =>
                i = (i64::from_str(value))
                return Ok(GlusterOption::ClientGraceTimeout(i))

            "cluster-self-heal-window-size" =>
                i = (u16::from_str(value))
                return Ok(GlusterOption::ClusterSelfHealWindowSize(i))

            "cluster-data-self-heal-algorithm" =>
                s = SelfHealAlgorithm::from_str(value)
                return Ok(GlusterOption::ClusterDataSelfHealAlgorithm(s))

            "cluster-min-free-disk" =>
                i = (u8::from_str(value))
                return Ok(GlusterOption::ClusterMinFreeDisk(i))

            "cluster-stripe-block-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::ClusterStripeBlockSize(i))

            "cluster-self-heal-daemon" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::ClusterSelfHealDaemon(t))

            "cluster-ensure-durability" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::ClusterEnsureDurability(t))

            "diagnostics-brick-log-level" =>
                l = log::LogLevel::from_str(value).unwrap_or(log::LogLevel::Debug)
                return Ok(GlusterOption::DiagnosticsBrickLogLevel(l))

            "diagnostics-client-log-level" =>
                l = log::LogLevel::from_str(value).unwrap_or(log::LogLevel::Debug)
                return Ok(GlusterOption::DiagnosticsClientLogLevel(l))

            "diagnostics-latency-measurement" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::DiagnosticsLatencyMeasurement(t))

            "diagnostics.count-fop-hits" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::DiagnosticsCountFopHits(t))

            "diagnostics.stats-dump-interval" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::DiagnosticsStatsDumpInterval(i))

            "diagnostics.fop-sample-buf-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::DiagnosticsFopSampleBufSize(i))

            "diagnostics.fop-sample-interval" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::DiagnosticsFopSampleInterval(i))

            "diagnostics.stats-dnscache-ttl-sec" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::DiagnosticsStatsDnscacheTtlSec(i))

            "diagnostics-dump-fd-stats" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::DiagnosticsDumpFdStats(t))

            "features-read-only" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::FeaturesReadOnly(t))

            "features-lock-heal" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::FeaturesLockHeal(t))

            "features-quota-timeout" =>
                i = (u16::from_str(value))
                return Ok(GlusterOption::FeaturesQuotaTimeout(i))

            "geo-replication-indexing" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::GeoReplicationIndexing(t))

            "network-frame-timeout" =>
                i = (u16::from_str(value))
                return Ok(GlusterOption::NetworkFrameTimeout(i))

            "nfs-enable-ino32" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsEnableIno32(t))

            "nfs-volume-access" =>
                s = AccessMode::from_str(value)
                return Ok(GlusterOption::NfsVolumeAccess(s))

            "nfs-trusted-write" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsTrustedWrite(t))

            "nfs-trusted-sync" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsTrustedSync(t))

            "nfs-export-dir" =>
                return Ok(GlusterOption::NfsExportDir(value))

            "nfs-export-volumes" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsExportVolumes(t))

            "nfs-rpc-auth-unix" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsRpcAuthUnix(t))

            "nfs-rpc-auth-null" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsRpcAuthNull(t))

            "nfs-ports-insecure" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsPortsInsecure(t))

            "nfs-addr-namelookup" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsAddrNamelookup(t))

            "nfs-register-with-portmap" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsRegisterWithPortmap(t))

            "nfs-disable" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::NfsDisable(t))

            "performance-write-behind-window-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::PerformanceWriteBehindWindowSize(i))

            "performance-io-thread-count" =>
                i = (u8::from_str(value))
                return Ok(GlusterOption::PerformanceIoThreadCount(i))

            "performance-flush-behind" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::PerformanceFlushBehind(t))

            "performance-cache-max-file-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::PerformanceCacheMaxFileSize(i))

            "performance-cache-min-file-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::PerformanceCacheMinFileSize(i))

            "performance-cache-refresh-timeout" =>
                i = (u8::from_str(value))
                return Ok(GlusterOption::PerformanceCacheRefreshTimeout(i))

            "performance-cache-size" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::PerformanceCacheSize(i))

            "performance-readdir-ahead" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::PerformanceReadDirAhead(t))

            "performance-parallel-readdir" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::PerformanceReadDirAhead(t))

            "performance-readdir-cache-limit" =>
                i = (u64::from_str(value))
                return Ok(GlusterOption::PerformanceReadDirAheadCacheLimit(i))

            "server.ssl" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::ServerSsl(t))

            "server-allow-insecure" =>
                t = Toggle::from_str(value)
                return Ok(GlusterOption::ServerAllowInsecure(t))

            "server-grace-timeout" =>
                i = (u16::from_str(value))
                return Ok(GlusterOption::ServerGraceTimeout(i))

            "server-statedump-path" =>
                p = PathBuf::from(value)
                return Ok(GlusterOption::ServerStatedumpPath(p))

            "ssl.certificate-depth" =>
                i = (u8::from_str(value))
                return Ok(GlusterOption::SslCertificateDepth(i))

            "ssl.cipher-list" =>
                return Ok(GlusterOption::SslCipherList(value))

            "storage-health-check-interval" =>
                i = (u16::from_str(value))
                return Ok(GlusterOption::StorageHealthCheckInterval(i))

            _ =>
                return Err(GlusterError::new(format!("Unknown option: ", s)))

# Custom error handling for the library
class GlusterError(Exception):
    def __init__(self, message, errors):
        super(GlusterError, self).__init__(message)
        self.errors = errors

    AddrParseError(std::net::AddrParseError),
    FromUtf8Error(std::string::FromUtf8Error),
    IoError(io::Error),
    NoVolumesPresent,
    ParseError(uuid::ParseError),
    ParseBoolErr(std::str::ParseBoolError),
    ParseIntError(std::num::ParseIntError),
    RegexError(regex::Error),
    SerdeError(serde_json::Error),


impl GlusterError
    # Create a new GlusterError with a String message
    def new(err: String) -> GlusterError
        GlusterError::IoError(io::Error::new(std::io::ErrorKind::Other, err))


    # Convert a GlusterError into a String representation.
    def to_string(self) -> String
        match *self
            GlusterError::IoError(ref err) => err.description(),
            GlusterError::FromUtf8Error(ref err) => err.description(),
            GlusterError::ParseError(ref err) => err.description(),
            GlusterError::AddrParseError(ref err) => err.description(),
            GlusterError::ParseIntError(ref err) => err.description(),
            GlusterError::ParseBoolErr(ref err) => err.description(),
            GlusterError::RegexError(ref err) => err.description(),
            GlusterError::NoVolumesPresent => "No volumes present",
            GlusterError::SerdeError(ref err) => err.description(),

class BrickStatus:
    def __init__(self, brick, tcp_port, rdma_port, online, pid):
        """
        brick: Brick,
        tcp_port: u16.  The tcp port
        rdma_port: u16. The rdma port
        online: bool. Whether the Brick is online or not
        pid: u16.  The process id of the Brick
        """
        self.brick = brick
        self.tcp_port = tcp_port
        self.rdma_port = rdma_port
        self.online = online
        self.pid = pid

    def cmp(self, other: BrickStatus) -> Ordering
        self.brick.peer.cmp(other.brick.peer)


    def partial_cmp(self, other: BrickStatus) -> Option<Ordering>
        Some(self.cmp(other))

# A Quota can be used set limits on the pool usage.  All limits are set in
# bytes.
class Quota:
    def __init__(self, path, limit, used):
        """
        path: String.  Filesystem path of the quota
        limit: u64. Byte limit
        used: u64. Bytes used.
        """
        self.path = path
        self.limit = limit
        self.used = used

def process_output(returncode, output):
    """
    Process a command output and check for success.
    :param returncode: Returncode from the command
    :param output:  Output from the command
    :return: Return 0 on success or stderr on failure
    """
    status = output.status
    if status.success():
        return 0
    else:
        return output.stderr

def run_command(command, arg_list, as_root, script_mode):
    """
    command: String.  The command to run
    arg_list: list. A list of arguments to add to the command
    as_root: bool.  Should the command be run as root
    script_mode: bool.  Should the command be run in script mode
    """
    if as_root:
        cmd = ["sudo"]
    else:
        cmd = [command]
    if script_mode:
        cmd.append("--mode=script")
    for arg in arg_list:
        cmd.append(arg)
    try:
        log("About to run command: {}".format(cmd))
        output = subprocess.check_output(cmd)
        return output
    except IOException:
        return None

# TODO: figure out a better way to do this.  This seems hacky
# Returns the local IPAddr address associated with this server
# # Failures
# Returns a GlusterError representing any failure that may have happened
# while trying to
# query this information.
def get_local_ip():
    default_route = ["route", "show", "0.0.0.0/0"]
    cmd_output = run_command("ip", default_route, False, False)
    default_route_stdout = cmd_output.stdout.decode('utf8')

    # default via 192.168.1.1 dev wlan0  proto static
    addr_regex = re.compile(r"(?P<addr>via \S+)")
    default_route_parse = match addr_regex.captures(default_route_stdout)
        Some(a) => a,
        None =>
            return GlusterError("Unable to parse default route from: {}".format(default_route_stdout))

    addr_raw = match default_route_parse.name("addr")
        Some(a) => a,
        None =>
            return GlusterError("Unable to find addr default route from: {}".format(default_route_stdout))

    # Skip "via" in the capture
    addr = addr_raw.split(" ").skip(1).collect()

    arg_list = ["route", "get", addr[0]]
    src_address_output = run_command("ip", arg_list, False, False)
    # 192.168.1.1 dev wlan0  src 192.168.1.7
    local_address_stdout = (String::from_utf8(src_address_output.stdout))
    src_regex = (Regex::new(r"(?P<src>src \S+)"))
    capture_output = match src_regex.captures(local_address_stdout)
        Some(a) => a,
        None =>
            return GlusterError("Unable to parse local_address from: {}".format(local_address_stdout))

    local_address_src = match capture_output.name("src")
        Some(a) => a,
        None =>
            return GlusterError("Unable to parse src from: {}".format(local_address_stdout))

    # Skip src in the capture
    local_ip = local_address_src.split(" ").skip(1).collect()
    ip_addr = local_ip[0].trim().parse::<IpAddr>()

    return ip_addr

# Resolves a str hostname into a ip address.
def resolve_to_ip(address):
    """
        address: String.  Hostname to resolve to an ip address
    """
    if address == "localhost":
        local_ip = (get_local_ip().map_err(|e| e))
        log("hostname is localhost.  Resolving to local ip ".format(local_ip))
        return local_ip

    arg_list = []
    arg_list.append(Vec<String> = Vec::new()("+short")
    # arg_list.append(("-x")
    arg_list.append((address.trim())
    output = run_command("dig", arg_list, False, False)

    status = output.status

    if status.success():
        output_str = output.stdout.decode('utf8')
        # Remove the trailing . and newline
        trimmed = output_str.trim().rstrip(".")
        return trimmed
    else:
        return output.stderr

def get_local_hostname():
    """
    A function to get the information from /etc/hostname
    """
    with open("/etc/hostname") as f:
        s = ""
        try:
            f.read_to_string(s)
        except IOException:
            pass
        return s.trim()
    return None

def translate_to_bytes(value):
    """
    This is a helper function to convert values such as 1PB into a bytes
    :rtype : float. Value in bytes or None if failed to parse
    """
    k = 1024
    if value.endswith("PB"):
        return float(value.rstrip("PB")) * k * k * k * k * k
    elif value.endswith("TB"):
        return float(value.rstrip("TB")) * k * k * k * k
    elif value.endswith("GB"):
        return float(value.rstrip("GB")) * k * k * k
    elif value.endswith("MB"):
        return float(value.rstrip("MB")) * k * k
    elif value.endswith("KB"):
        return float(value.rstrip("KB")) * k
    elif value.endswith("Bytes"):
        return float(value.rstrip("BYTES"))
    else:
        return None

# Return all bricks that are being served locally in the volume
def get_local_bricks(volume):
    """
        volume: Name of the volume to get local bricks for
    """
    try:
        vol_info = volume_info(volume)
        local_ip = get_local_ip()
        bricks: Vec<Brick> = vol_info.bricks
            .iter()
            .filter(|brick| brick.peer.hostname == local_ip)
            .map(|brick| brick.clone())
            .collect()
        return bricks
    except Exception:
        raise
