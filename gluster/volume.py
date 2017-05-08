from enum import Enum
import ipaddress
# import std::path::Path, PathBuf

from peer import get_peer, Peer, State
import re
# import serde_xml_rs::deserialize
from lib import BitrotOption, BrickStatus, GlusterError, GlusterOption, process_output, Quota, resolve_to_ip, run_command, translate_to_bytes
import uuid

# A Gluster Brick consists of a Peer and a path to the mount point
class Brick:
    def __init__(self, peer, path):
        self.peer = peer#: Peer,
        self.path = path#: PathBuf,

    # Returns a String representation of the selected enum variant.
    def to_string(self):
        return ":".format(self.peer.hostname, self.path)

# An enum to select the transport method Gluster should import for the Volume
class Transport(Enum):
    Tcp = "tcp"
    TcpAndRdma = "tcp,rdma"
    Rdma = "rdma"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(transport):
        if transport == "tcp":
            return Transport.Tcp
        elif transport == "tcp,rdma":
            return Transport.TcpAndRdma
        elif transport == "rdma":
            return Transport.Rdma

class VolumeTranslator(Enum):
    Arbiter = "arbiter"
    Disperse = "disperse"
    Replica = "replica"
    Redundancy = "redundancy"
    Stripe = "stripe"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(translator):
        if translator == "arbiter":
            return VolumeTranslator.Arbiter
        elif translator == "disperse":
            return VolumeTranslator.Disperse
        elif translator == "replica":
            return VolumeTranslator.Replica
        elif translator == "redundancy":
            return VolumeTranslator.Redundancy
        elif translator == "stripe":
            return VolumeTranslator.Stripe
        else:
            return None


# These are all the different Volume types that are possible in Gluster
# Note: Tier is not represented here becaimport I'm waiting for it to become
# more stable
# For more information about these types see: [Gluster Volume]
# (https:#gluster.readthedocs.
# org/en/latest/Administrator%20Guide/Setting%20Up%20Volumes/)
 class VolumeType(Enum):
    Arbiter = "arbiter"
    Distribute ="distribute"
    Stripe = "stripe"
    Replicate = "replicate"
    StripedAndReplicate = "stripd-replicate"
    Disperse = "disperse"
    # Tier,
    DistributedAndStripe = "distributed-stripe"
    DistributedAndReplicate = "distributed-replicate"
    DistributedAndStripedAndReplicate = "distributed-striped-replicate"
    DistributedAndDisperse = "distributed-disperse"

    def __str__(self):
        return "{}".format(self.value)

    # Returns a enum variant of the given String
    @staticmethod
    def from_str(vol_type):
        if vol_type == "Arbiter":
            return VolumeType.Arbiter
        elif vol_type == "Distribute":
           return VolumeType.Distribute
        elif vol_type == "Stripe":
           return VolumeType.Stripe,
        elif vol_type == "Replicate":
           return VolumeType.Replicate
        elif vol_type == "Striped-Replicate":
           return VolumeType.StripedAndReplicate
        elif vol_type == "Disperse":
           return VolumeType.Disperse
           # VolumeType::Tier => "Tier", #TODO: Waiting for this to become stable
        elif vol_type == "Distributed-Stripe":
           return VolumeType.DistributedAndStripe
        elif vol_type == "Distributed-Replicate":
           return VolumeType.DistributedAndReplicate
        elif vol_type == "Distributed-Striped-Replicate":
           return VolumeType.DistributedAndStripedAndReplicate
        elif vol_type == "Distrubted-Disperse":
           return VolumeType.DistributedAndDisperse
        else:
           return None

#[test]
def test_xml_parser()
    import std::fs::File
    import std::io::Read
    input =
        buff = String::new()
        f = File::open("tests/replicated-vol.xml").unwrap()
        f.read_to_string(buff).unwrap()
        buff

    volume: Result<XmlVol, ::serde_xml_rs::Error> = deserialize(input.as_bytes())
    println!("Volume: :?", volume)


class StructOptions
    option: Vec<XmlOption>,


class XmlOption
    name: String,
    value: String,


class StructBricks
    brick: Vec<XmlBrick>,


class XmlBrick
    name: String,
    hostUuid: String,
    isArbiter: i32,


class XVol
    volume: XmlVolume,


class XmlVolume
    name: String,
    id: String,
    status: i32,
    statusStr: String,
    snapshotCount: u64,
    brickCount: u64,
    distCount: u64,
    stripeCount: u64,
    replicaCount: u64,
    arbiterCount: u64,
    disperseCount: u64,
    redundancyCount: u64,
    #[serde(rename = "type")]
    vol_type: u32,
    typeStr: String,
    transport: u32,
    xlators: Option<String>,
    bricks: StructBricks,
    optCount: u64,
    options: StructOptions,


class XmlVolInfo:
    def __init__(self):
        pass
    volumes: Vec<XVol>, # count: u32,


class XmlVol:
    def __init__(self):
        pass
    opRet: i32,
    opErrno: i32,
    opErrstr: String,
    volInfo: XmlVolInfo,


# A volume is a logical collection of bricks. Most of the gluster management
# operations
# happen on the volume.
class Volume:
    def __init__(self, name, vol_type, id, status, transport, bricks, options):
        """
        :param name: String.  Name of the volume
        :param vol_type: VolumeType.
        :param id: uuid
        :param status: String.  Status of the volume
        :param transport: Transport.  Transport protocol
        :param bricks: list.  List of Brick
        :param options: dict.  String:String mapping of volume options
        """
        self.name = name
        self.vol_type = vol_type
        self.id = id
        self.status = status
        self.transport = transport
        self.bricks = bricks
        self.options = options

# Volume Name: test
# Type: Replicate
# Volume ID: cae6868d-b080-4ea3-927b-93b5f1e3fe69
# Status: Started
# Number of Bricks: 1 x 2 = 2
# Transport-type: tcp
# Bricks:
# Brick1: 172.31.41.135:/mnt/xvdf
# Brick2: 172.31.26.65:/mnt/xvdf
# Options Reconfigured:
# features.inode-quota: off
# features.quota: off
# transport.address-family: inet
# performance.readdir-ahead: on
# nfs.disable: on
#
class ParseState(Enum):
    Root=0,
    Bricks=1,
    Options=2,


def volume_list() -> Option<Vec<String>>
    """
    # Lists all available volume names.
    # # Failures
    # Will return None if the Volume list command failed or if volume could not
    # be transformed
    # into a String from utf8
    """
    arg_list = ["volume", "list"]
    output = run_command("gluster", arg_list, True, False)
    status = output.status

    if not status.success():
        log("Volume list get command failed")
        return None
    output_str = output.stdout.decode('utf8')
    volume_names = []
    for line in output_str.lines():
        if len(line) == 0:
            # Skip any blank lines in the output
            continue
        volume_names.append(line.trim())
    return volume_names

"""
#[test]
def test_parse_volume_info():
    test_data = r#"

Volume Name: test
Type: Replicate
Volume ID: cae6868d-b080-4ea3-927b-93b5f1e3fe69
Status: Started
Number of Bricks: 1 x 2 = 2
Transport-type: tcp
Bricks:
Brick1: 172.31.41.135:/mnt/xvdf
Options Reconfigured:
features.inode-quota: off
features.quota: off
transport.address-family: inet
performance.readdir-ahead: on
nfs.disable: on
"#
    result = parse_volume_info("test", test_data).unwrap()
    options_map: BTreeMap<String, String> = BTreeMap::new()
    options_map.insert("features.inode-quota", "off")
    options_map.insert("features.quota", "off")
    options_map.insert("transport.address-family", "inet")
    options_map.insert("performance.readdir-ahead", "on")
    options_map.insert("nfs.disable", "on")

    vol_info = Volume
        name: "test",
        vol_type: VolumeType::Replicate,
        id: Uuid::parse_str("cae6868d-b080-4ea3-927b-93b5f1e3fe69").unwrap(),
        status: "Started",
        transport: Transport::Tcp,
        bricks: vec![Brick
                         peer: Peer
                             uuid: Uuid::parse_str("78f68270-201a-4d8a-bad3-7cded6e6b7d8").unwrap(),
                             hostname: "test_ip",
                             status: State::Connected,
                         ,
                         path: PathBuf::from("/mnt/xvdf"),
                     ],
        options: options_map,

    println!("vol_info: :?", vol_info)
    assert_eq!(vol_info, result)
"""

def parse_volume_info(volume, output_str) -> Result<Volume, GlusterError>:
    """
    # Variables we will return in a class
    """
    transport_type = ""
    volume_type = ""
    volume_name = ""
    volume_options: BTreeMap<String, String> = BTreeMap::new()
    status = ""
    bricks: Vec<Brick> = []
    id = Uuid::nil()

    if output_str.trim() == "No volumes present":
        log("No volumes present")
        raise GlusterError.NoVolumesPresent

    if output_str.trim() == "Volume {} does not exist".format(volume):
        log("Volume  does not exist", volume)
        raise GlusterError("Volume: {} does not exist".format(volume))

    parser_state = ParseState::Root

    for line in output_str.lines():
        if line.is_empty():
            # Skip the first blank line in the output
            continue

        match line
            "Bricks:" =>
                parser_state = ParseState::Bricks
                continue

            "Options Reconfigured:" =>
                parser_state = ParseState::Options
                continue

            _ =>

        match parser_state
            ParseState::Root =>
                parts: Vec<String> = line.split(": ").map(|e| e).collect()
                if parts.len() < 2
                    # We don't know what this is
                    continue

                ref name = parts[0]
                ref value = parts[1]

                if name == "Volume Name":
                    volume_name = value
                if name == "Type":
                    volume_type = value
                if name == "Volume ID":
                    id = (Uuid::parse_str(value))
                if name == "Status":
                    status = value
                if name == "Transport-Type":
                    transport_type = value
                if name == "Number of Bricks":
                    pass

            ParseState::Bricks =>
                parts: Vec<String> = line.split(": ").map(|e| e).collect()
                if parts.len() < 2
                    # We don't know what this is
                    continue

                ref value = parts[1]

                # brick_str = value
                brick_parts: Vec<str> = value.split(":").collect()
                assert!(brick_parts.len() == 2,
                        "Failed to parse bricks from gluster vol info")

                hostname = brick_parts[0].trim()

                # Translate back into an IP address if needed
                check_for_ip = hostname.parse::<IpAddr>()

                if check_for_ip.is_err()
                    # It's a hostname so lets resolve it
                    hostname = match resolve_to_ip(hostname)
                        Ok(ip_addr) => ip_addr,
                        Err(e) =>
                            raise GlusterError("Failed to resolve hostname: {}"
                                                                 ". Error: {}".format(hostname, e))
                peer= get_peer(hostname)
                log("get_peer_by_ipaddr result: Peer: {}".format(peer))
                brick = Brick(
                    # Should this panic if it doesn't work?
                    peer=peer,
                    path=PathBuf::from(brick_parts[1]))
                bricks.push(brick)

            ParseState::Options =>
                # Parse the options
                parts: Vec<String> = line.split(": ").map(|e| e).collect()
                if parts.len() < 2
                    # We don't know what this is
                    continue

                volume_options.insert(parts[0].clone(), parts[1].clone())

    transport = Transport::new(transport_type)
    vol_type = VolumeType::new(volume_type)
    vol_info = Volume
        name: volume_name,
        vol_type: vol_type,
        id: id,
        status: status,
        transport: transport,
        bricks: bricks,
        options: volume_options,

    return Ok(vol_info)


def volume_info(volume):
    """
    Returns a Volume with all available information on the volume
    volume: String.  The volume to gather info about
    :return: Volume.  The volume information
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "info", volume]
    output = run_command("gluster", arg_list, True, False)
    status = output.status

    if !status.success():
        log("Volume info get command failed: {}".format(output.stdout.decode('utf8')))
        # TODO: What is the appropriate error to report here?
        # The client is using this to figure out if it should make a volume
        raise GlusterError::NoVolumesPresent
    output_str = output.stdout.decode('utf8')

    return parse_volume_info(volume, output_str)

"""
# Returns a u64 representing the bytes importd on the volume.
# Note: This imports my brand new RPC library.  Some bugs may exist so import
# caution.  This does not
# shell out and therefore should be significantly faster.  It also suffers
# far less hang conditions
# than the CLI version.
# # Failures
# Will return GlusterError if the RPC fails
 def get_quota_usage(volume: str) -> Result<u64, GlusterError>
    xid = 1 #Transaction ID number.
    prog = rpc::GLUSTER_QUOTA_PROGRAM_NUMBER
    vers = 1 #RPC version == 1

    verf = rpc::GlusterAuth
        flavor: rpc::AuthFlavor::AuthNull,
        stuff: vec![0, 0, 0, 0],

    verf_bytes = (verf.pack())

    creds = rpc::GlusterCred
        flavor: rpc::GLUSTER_V2_CRED_FLAVOR,
        pid: 0,
        uid: 0,
        gid: 0,
        groups: "",
        lock_owner: vec![0, 0, 0, 0],

    cred_bytes = (creds.pack())

    call_bytes = (rpc::pack_quota_callheader(xid,
                                        prog,
                                        vers,
                                        rpc::GlusterAggregatorCommand::GlusterAggregatorGetlimit,
                                        cred_bytes,
                                        verf_bytes))

    dict: HashMap<String, Vec<u8>> = HashMap::with_capacity(4)

    # TODO: Make a Gluster wd RPC call and parse this from the quota.conf file
    # This is crap
    gfid = "00000000-0000-0000-0000-000000000001".into_bytes()
    gfid.push(0) #Null Terminate
    name = volume.into_bytes()
    name.push(0) #Null Terminate
    version = "1.20000005".into_bytes()
    version.push(0) #Null Terminate
    #No idea what vol_type == 5 means to Gluster
    vol_type = "5".into_bytes()
    vol_type.push(0) #Null Terminate

    dict.insert("gfid", gfid)
    dict.insert("type", vol_type)
    dict.insert("volume-uuid", name)
    dict.insert("version", version)
    quota_request = rpc::GlusterCliRequest  dict: dict
    quota_bytes = (quota_request.pack())
    for byte in quota_bytes
        call_bytes.push(byte)


    # Ok.. we need to hunt down the quota socket file ..crap..
    addr = Path::new("/var/run/gluster/quotad.socket")
    sock = (UnixStream::connect(addr))

    send_bytes = (rpc::sendrecord(sock, call_bytes))
    reply_bytes = (rpc::recvrecord(sock))

    cursor = Cursor::new(reply_bytes[..])

    # Check for success
    (rpc::unpack_replyheader(cursor))

    cli_response = (rpc::GlusterCliResponse::unpack(cursor))
    # The raw bytes
    quota_size_bytes = match cli_response.dict.get_mut("trusted.glusterfs.quota.size")
        Some(s) => s,
        None =>
            return Err(GlusterError::new("trusted.glusterfs.quota.size was not returned from #
                                          quotad"
                ))


    # Gluster is crazy and encodes a ton of data in this vector.  We're just going
    # to
    # read the first value and throw away the rest.  Why they didn't just import a
    # class and
    # XDR is beyond me
    size_cursor = Cursor::new(quota_size_bytes[..])
    usage = (size_cursor.read_u64::<BigEndian>())
    return Ok(usage)
"""

def quota_list(volume):
    """
    Return a list of quotas on the volume if any
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: List of Quota's on the volume
    :raises: GlusterError if the command fails to run
    """
    args_list = ["volume", "quota", volume, "list"]
    output = run_command("gluster", args_list, True, False)
    status = output.status

    if !status.success():
        "Volume quota list command failed with error: {}".format(output.stderr.decode('utf8'))
        raise GlusterError(output.stderr.decode('utf8'))

    output_str = output.stdout.decode('utf8')
    quota_list = parse_quota_list(volume, output_str)

    return quota_list

"""
#[test]
def test_quota_list()
    test_data = r#"
    Path Hard-limit  Soft-limit      Used  Available  Soft-limit exceeded? Hard-limit exceeded?
----------------------------------------------------------------------------------------------
/ 1.0KB  80%(819Bytes)   0Bytes   1.0KB              No                   No
"#
    result = parse_quota_list("test", test_data)
    quotas = vec![Quota
                          path: PathBuf::from("/"),
                          limit: 1024,
                          importd: 0,
                      ]
    println!("quota_list: :?", result)
    assert_eq!(quotas, result)
"""

# Return a list of quotas on the volume if any
# ThinkPad-T410s:~# gluster vol quota test list
# Path                   Hard-limit Soft-limit   Used  Available  Soft-limit
# exceeded? Hard-limit exceeded?
# ---------------------------------------------------------------------------
# /                                        100.0MB       80%      0Bytes
# 100.0MB              No                   No
#
# There are 2 ways to get quota information
# 1. List the quota's with the quota list command.  This command has been
# known in the past to hang
# in certain situations.
# 2. Issue an RPC directly to Gluster
#
def parse_quota_list(volume, output_str):
    """
    Parse a quota list command output and return a list of quotas
    volume: String.  The volume to parse quotas from
    # ThinkPad-T410s:~# gluster vol quota test list
    # Path                   Hard-limit Soft-limit   Used  Available  Soft-limit
    # exceeded? Hard-limit exceeded?
    # --------------------------------------------------------------------------
    # /                                        100.0MB       80%      0Bytes
    # 100.0MB              No                   No
    #
    # There are 2 ways to get quota information
    # 1. List the quota's with the quota list command.  This command has been
    # known in the past to hang
    # in certain situations.
    # 2. Go to the backend brick and getfattr -d -e hex -m . dir_name/ on the
    # directory directly:
    # /mnt/x1# getfattr -d -e hex -m . quota/
    # # file: quota/
    # trusted.gfid=0xdb2443e4742e4aaf844eee40405ad7ae
    # trusted.glusterfs.dht=0x000000010000000000000000ffffffff
    # trusted.glusterfs.quota.00000000-0000-0000-0000-000000000001.
    # contri=0x0000000000000000
    # trusted.glusterfs.quota.dirty=0x3000
    # trusted.glusterfs.quota.limit-set=0x0000000006400000ffffffffffffffff
    # trusted.glusterfs.quota.size=0x0000000000000000
    """
    quota_list = []

    if output_str.trim() == "quota: No quota configured on volume {}".format(volume):
        return quota_list

    for line in output_str.lines():
        if line.is_empty():
            # Skip the first blank line in the output
            continue

        if line.starts_with(" "):
            continue

        if line.starts_with("-"):
            continue

        # Ok now that we've eliminated the garbage
        parts = line.split(" ").filter(|s| !s.is_empty())
        # Output should match: ["/", "100.0MB", "80%", "0Bytes", "100.0MB", "No", "No"]
        if parts.len() > 3:
            limit: f64 = match translate_to_bytes(parts[1])
                Some(v) => v,
                None => 0.0,

            importd: f64 = match translate_to_bytes(parts[3])
                Some(v) => v,
                None => 0.0,

            quota = Quota
                path: PathBuf::from(parts[0]),
                limit: limit as u64,
                importd: importd as u64,

            quota_list.push(quota)

        # else?
    return quota_list


def volume_enable_bitrot(volume):
    """
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, "enable"]
    return process_output(run_command("gluster", arg_list, True, False))


def volume_disable_bitrot(volume):
    """
    Disable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, "disable"]
    return process_output(run_command("gluster", arg_list, True, False))

def volume_set_bitrot_option(volume, setting):
    """
    Set a bitrot option on the volume
    volume: String.  The volume to operate on.
    setting: BitrotOption.  The option to set on the bitrot daemon
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, setting, setting.value()]
    return process_output(run_command("gluster", arg_list, True, True))

def volume_enable_quotas(volume):
    """
    Enable quotas on the volume
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "enable"]
    return process_output(run_command("gluster", arg_list, True, False))

def volume_quotas_enabled(volume):
    """
     Check if quotas are already enabled on a volume
    :return: bool.  True/False if quotas are enabled
    :raises: GlusterError if the command fails to run
    """
    vol_info = volume_info(volume)
    quota = vol_info.options.get("features.quota")
    if quota:
        Some(v) =>
            if v == "off":
                return False
             elif v == "on":
                return True
             else:
                # No idea what this is
                return False

        None => False,

def volume_disable_quotas(volume):
    """
    Disable quotas on the volume
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "disable"]
    return process_output(run_command("gluster", arg_list, True, False))


def volume_remove_quota(volume, path):
    """
    Removes a size quota to the volume and path.
    path: String.  Path of the directory to remove a quota on
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    path_str = path.to_string_lossy()
    arg_list = ["volume", "quota", volume, "remove", path_str]
    return process_output(run_command("gluster", arg_list, True, False))

def volume_add_quota(volume, path, size):
    """
    Adds a size quota to the volume and path.
    volume: String Volume to add a quota to
    path: String.  Path of the directory to apply a quota on
    size: int. Size in bytes of the quota to apply
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    path_str = path.to_string_lossy()
    size_string = size
    arg_list = ["volume", "quota", volume, "limit-usage", path_str, size_string]

    return process_output(run_command("gluster", arg_list, True, False))

"""
#[test]
def test_parse_volume_status()
    test_data = r#"
    Gluster process                             TCP Port  RDMA Port  Online  Pid
    ------------------------------------------------------------------------------
    Brick 172.31.46.33:/mnt/xvdf                49152     0          Y       14228
    Brick 172.31.19.130:/mnt/xvdf               49152     0          Y       14446
    Self-heal Daemon on localhost               N/A       N/A        Y       14248
    Self-heal Daemon on ip-172-31-19-130.us-wes
    t-2.compute.internal                        N/A       N/A        Y       14466

    Task Status of Volume test
    ------------------------------------------------------------------------------
    There are no active volume tasks

"#
    result = parse_volume_status(test_data).unwrap()
    println!("status: :?", result)
    # Have to inspect these manually becaimport the UUID is randomly generated by the parser.
    # It's either that or it has to be set to some fixed UUID.  Neither solution seems good
    assert_eq!(result[0].brick.peer.hostname, "172.31.46.33")
    assert_eq!(result[0].tcp_port, 49152)
    assert_eq!(result[0].rdma_port, 0)
    assert_eq!(result[0].online, True)
    assert_eq!(result[0].pid, 14228)

    assert_eq!(result[1].brick.peer.hostname, "172.31.19.130")
    assert_eq!(result[1].tcp_port, 49152)
    assert_eq!(result[1].rdma_port, 0)
    assert_eq!(result[1].online, True)
    assert_eq!(result[1].pid, 14446)

"""

# Based on the replicas or erasure bits that are still available in the
# volume this will return
# True or False as to whether you can remove a Brick. This should be called
# before volume_remove_brick()
def ok_to_remove(volume, brick):# -> Result<bool, GlusterError>
    """
    Based on the replicas or erasure bits that are still available in the
    volume this will return
    True or False as to whether you can remove a Brick. This should be called
    before volume_remove_brick()
    volume: String. Volume to check if the brick is ok to remove
    brick: Brick. Brick to check
    """
    arg_list = ["vol", "status", volume]

    output = run_command("gluster", arg_list, True, False)
    if !output.status.success():
        stderr = output.stderr.decode('utf8')
        raise GlusterError(stderr)


    output_str = output.stdout.decode('utf8')
    bricks = parse_volume_status(output_str)
    # The redudancy requirement is needed here.
    # The code needs to understand what
    # volume type it's operating on.
    return True


#  def volume_shrink_replicated(volume: str,
# replica_count: usize,
# bricks: Vec<Brick>,
# force: bool) -> Result<i32,String>
# volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
# <start|stop|status|c
# ommit|force> - remove brick from volume <VOLNAME>
#
#
def parse_volume_status(output_str: String) -> Result<Vec<BrickStatus>, GlusterError>
    # Sample output
    # Status of volume: test
    # Gluster process                             TCP Port  RDMA Port  Online  Pid
    # ------------------------------------------------------------------------------
    # Brick 192.168.1.6:/mnt/brick2               49154     0          Y
    # 14940
    # Brick 192.168.1.6:/mnt/brick3               49155     0          Y
    # 14947
    #
    bricks: Vec<BrickStatus> = Vec::new()
    for line in output_str.lines()
        # Skip the header crap
        if line.starts_with("Status")
            continue

        if line.starts_with("Gluster")
            continue

        if line.starts_with("-")
            continue

        regex_str = r#"Brick#s+(?P<hostname>[a-zA-Z0-9.]+)
:(?P<path>[/a-zA-z0-9]+)
#s+(?P<tcp>[0-9]+)#s+(?P<rdma>[0-9]+)#s+(?P<online>[Y,N])#s+(?P<pid>[0-9]+)"#
        brick_regex = (Regex::new(regex_str.replace("#n", "")))
        match brick_regex.captures(line)
            Some(result) =>
                tcp_port = match result.name("tcp")
                    Some(port) => port,
                    None =>
                        return Err(GlusterError::new("Unable to find tcp port in gluster vol #
                                                      status output"
                            ))



                peer = Peer
                    uuid: Uuid::new_v4(),
                    hostname: result.name("hostname").unwrap(),
                    status: State::Unknown,


                brick = Brick
                    peer: peer,
                    path: PathBuf::from(result.name("path").unwrap()),


                online = match result.name("online").unwrap()
                    "Y" => True,
                    "N" => False,
                    _ => False,


                status = BrickStatus
                    brick: brick,
                    tcp_port: (u16::from_str(result.name("tcp").unwrap())),
                    rdma_port: (u16::from_str(result.name("rdma").unwrap())),
                    online: online,
                    pid: (u16::from_str(result.name("pid").unwrap())),

                bricks.push(status)

            None =>


    return Ok(bricks)

def volume_status(volume):
    """
        Query the status of the volume given.
        :return: list.  List of BrickStatus
        :raise: Raises GlusterError on exception
    """
    arg_list = ["vol", "status", volume]

    output = run_command("gluster", arg_list, True, False)
    if !output.status.success():
        stderr = output.stderr.decode('utf8')
        raise GlusterError(stderr)
    output_str = output.stdout.decode('utf8')
    bricks = parse_volume_status(output_str)

    return bricks

#  def volume_shrink_replicated(volume: str,
# replica_count: usize,
# bricks: Vec<Brick>,
# force: bool) -> Result<i32,String>
# volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
# <start|stop|status|c
# ommit|force> - remove brick from volume <VOLNAME>
#
#

# This will remove a brick from the volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_remove_brick(volume, bricks, force):
    """
    This will remove bricks from the volume
    :param volume: String of the volume to remove bricks from.
    :param bricks:  list.  List of bricks to remove from the volume
    :param force:  bool.  Force remove brick
    :return: int.  Negative number on error
    """

    if len(bricks) == 0:
        raise GlusterError("The brick list is empty.  Not expanding volume")

    for brick in bricks:
        ok = ok_to_remove(volume, brick)
        if ok:
            arg_list = ["volume", "remove-brick", volume]
            if force:
                arg_list.append("force")

            arg_list.append("start")
            status = process_output(run_command("gluster", arg_list, True, True))
        else:
            raise GlusterError("Unable to remove brick due to redundancy failure")
    return 0


# # Failures
# Will return GlusterError if the command fails to run
def volume_add_brick(volume, bricks, force):
    """
    volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
    <NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
    This adds a new brick to the volume
    :param volume: String of the volume to add bricks to.
    :param bricks:  list.  List of bricks to add to the volume
    :param force:  bool.  Force add brick
    :return: int.  Negative number on error
    """

    if len(bricks) == 0:
        raise GlusterError("The brick list is empty.  Not expanding volume")
    arg_list = ["volume", "add-brick", volume]
    for brick in bricks.iter():
        arg_list.push(brick)

    if force:
        arg_list.push("force")

    return process_output(run_command("gluster", arg_list, True, True))


def volume_start(volume, force):
    # Should I check the volume exists first?
    """
    Once a volume is created it needs to be started.  This starts the volume
    :param volume: String of the volume to start.
    :param force:  bool.  Force start
    :return: int.  Negative number on error
    """
    arg_list = ["volume", "start", volume]

    if force:
        arg_list.push("force")

    return process_output(run_command("gluster", arg_list, True, True))


def volume_stop(volume, force):
    """
    This stops a running volume
    :param volume:  String of the volume to stop
    :param force:  bool. Force stop.
    :return: int.  Negative number on error
    """
    arg_list = ["volume", "stop", volume]

    if force:
        arg_list.push("force")

    return process_output(run_command("gluster", arg_list, True, True))

def volume_delete(volume):
    """
    This deletes a stopped volume
    :param volume:  String of the volume name to delete
    :return:  int.  Negative number on error
    """
    arg_list = ["volume", "delete", volume]
    return process_output(run_command("gluster", arg_list, True, True))

## END DAY 1

# This function doesn't do anything yet.  It is a place holder becaimport
# volume_rebalance
# is a long running command and I haven't decided how to poll for completion
# yet
def volume_rebalance(volume):
    # Usage: volume rebalance <VOLNAME> fix-layout start | start
    # [force]|stop|status
    pass


def volume_create<T: ToString>(volume: str,
                              options: HashMap<VolumeTranslator, T>,
                              transport: Transport,
                              bricks: Vec<Brick>,
                              force: bool)
                              -> Result<i32, GlusterError>

    if bricks.is_empty()
        return Err(GlusterError::new("The brick list is empty. Not creating volume"))


    # TODO: figure out how to check each VolumeTranslator type
    # if (bricks.len() % replica_count) != 0
    # return Err("The brick list and replica count are not multiples. Not creating
    # volume")
    #
    #

    arg_list = []
    arg_list.push("volume")
    arg_list.push("create")
    arg_list.push(volume)

    for (key, value) in options.iter()
        arg_list.push(key.clone())
        arg_list.push(value)


    arg_list.push("transport")
    arg_list.push(transport.clone())

    for brick in bricks.iter()
        arg_list.push(brick)

    if force
        arg_list.push("force")

    return process_output(run_command("gluster", arg_list, True, True))


def vol_set(volume: str, option: GlusterOption) -> Result<i32, GlusterError>
    arg_list = ["volume", "set", volume]
    arg_list.push(option)
    arg_list.push(option.value())
    return process_output(run_command("gluster", arg_list, True, True))


# Set an option on the volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_set_options(volume: str, settings: Vec<GlusterOption>) -> Result<i32, GlusterError>
    results: Vec<Result<i32, GlusterError>> =
        settings.iter().map(|gluster_opt| vol_set(volume, gluster_opt)).collect()

    error_list: Vec<String> = Vec::new()
    for result in results:
        match result
            Ok(_) =>
            Err(e) => error_list.push(e),


    if error_list.len() > 0:
        return Err(GlusterError::new(error_list.join("#n")))


    return Ok(0)



# This creates a new replicated volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_replicated(volume: str,
                                replica_count: usize,
                                transport: Transport,
                                bricks: Vec<Brick>,
                                force: bool)
                                -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new()
    volume_translators.insert(VolumeTranslator::Replica, replica_count)

    return volume_create(volume, volume_translators, transport, bricks, force)


# The arbiter volume is special subset of replica volumes that is aimed at preventing
# split-brains and providing the same consistency guarantees as a normal replica 3 volume
# without consuming 3x space.
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_arbiter(volume: str,
                             replica_count: usize,
                             arbiter_count: usize,
                             transport: Transport,
                             bricks: Vec<Brick>,
                             force: bool)
                             -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new()
    volume_translators.insert(VolumeTranslator::Replica, replica_count)
    volume_translators.insert(VolumeTranslator::Arbiter, arbiter_count)

    return volume_create(volume, volume_translators, transport, bricks, force)



# This creates a new striped volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_striped(volume: str,
                             stripe: usize,
                             transport: Transport,
                             bricks: Vec<Brick>,
                             force: bool)
                             -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new()
    volume_translators.insert(VolumeTranslator::Stripe, stripe)

    return volume_create(volume, volume_translators, transport, bricks, force)


# This creates a new striped and replicated volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_striped_replicated(volume: str,
                                        stripe: usize,
                                        replica: usize,
                                        transport: Transport,
                                        bricks: Vec<Brick>,
                                        force: bool)
                                        -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new()
    volume_translators.insert(VolumeTranslator::Stripe, stripe)
    volume_translators.insert(VolumeTranslator::Replica, replica)

    return volume_create(volume, volume_translators, transport, bricks, force)


# This creates a new distributed volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_distributed(volume: str,
                                 transport: Transport,
                                 bricks: Vec<Brick>,
                                 force: bool)
                                 -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, String> = HashMap::new()

    return volume_create(volume, volume_translators, transport, bricks, force)

# This creates a new erasure coded volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_create_erasure(volume: str,
                             disperse: usize,
                             redundancy: usize,
                             transport: Transport,
                             bricks: Vec<Brick>,
                             force: bool)
                             -> Result<i32, GlusterError>

    volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new()
    volume_translators.insert(VolumeTranslator::Disperse, disperse)
    volume_translators.insert(VolumeTranslator::Redundancy, redundancy)

    return volume_create(volume, volume_translators, transport, bricks, force)
