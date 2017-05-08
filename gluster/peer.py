from enum import Enum
import re
from lib import GlusterError, process_output, resolve_to_ip, run_command
import uuid

"""
#[test]
fn test_parse_peer_status()
    test_result =
        vec![Peer
                 uuid: Uuid::parse_str("afbd338e-881b-4557-8764-52e259885ca3").unwrap(),
                 hostname: "10.0.3.207",
                 status: State::PeerInCluster,
             ,
             Peer
                 uuid: Uuid::parse_str("fa3b031a-c4ef-43c5-892d-4b909bc5cd5d").unwrap(),
                 hostname: "10.0.3.208",
                 status: State::PeerInCluster,
             ,
             Peer
                 uuid: Uuid::parse_str("5f45e89a-23c1-41dd-b0cd-fd9cf37f1520").unwrap(),
                 hostname: "10.0.3.209",
                 status: State::PeerInCluster,
             ]
    test_line = r#"Number of Peers: 3 Hostname: 10.0.3.207
Uuid: afbd338e-881b-4557-8764-52e259885ca3 State: Peer in Cluster (Connected)
Hostname: 10.0.3.208 Uuid: fa3b031a-c4ef-43c5-892d-4b909bc5cd5d
State: Peer in Cluster (Connected) Hostname: 10.0.3.209
Uuid: 5f45e89a-23c1-41dd-b0cd-fd9cf37f1520 State: Peer in Cluster (Connected)"#


    # Expect a 3 peer result
    result = parse_peer_status(test_line)
    println!("Result: :?", result)
    assert!(result.is_ok())

    result_unwrapped = result.unwrap()
    assert_eq!(test_result, result_unwrapped)

"""

# A enum representing the possible States that a Peer can be in
class State(Enum):
    Connected = "connected",
    Disconnected = "disconnected"
    Unknown = ""
    EstablishingConnection = "establishing connection"
    ProbeSentToPeer = "probe sent to peer"
    ProbeReceivedFromPeer = "probe received from peer"
    PeerInCluster = "peer in cluster"
    AcceptedPeerRequest = "accepted peer in cluster"
    SentAndReceivedPeerRequest = "sent and received peer request"
    PeerRejected = "peer rejected"
    PeerDetachInProgress = "peer detach in progress"
    ConnectedToPeer = "connected to peer"
    PeerIsConnectedAndAccepted = "peer is connected and accepted"
    InvalidState = "invalid state"
    def __str__(self):
        return "{}".format(self.value)

class Peer(object):
    def __init__(self, uuid, hostname, status):
        """
        A Gluster Peer.  A Peer is roughly equivalent to a server in Gluster.
        :param uuid: Unique identifier of this peer
        :param hostname:  hostname or ip address of the peer
        :param status:  current State of the peer
        """
        self.uuid = uuid
        self.hostname = hostname
        self.status = status

   def cmp(self, other: Peer):
        self.uuid.cmp(other.uuid)
   def partial_cmp(self, other: Peer):
        Some(self.cmp(other))
   def fmt(self):
       return "UUID: {}  Hostname: {} Status: {}".format(self.uuid.hyphenated(),self.hostname, self.status)

def get_peer(hostname): # -> Result<Peer, GlusterError>
    """
    This will query the Gluster peer list and return a Peer class for the peer
    # Failures
    Returns GlusterError if the peer could not be found
    """

    """
    if cfg!(test)
        return Ok(Peer
            uuid: Uuid::parse_str("78f68270-201a-4d8a-bad3-7cded6e6b7d8").unwrap(),
            hostname: "test_ip",
            status: State::Connected,
        )
    """
    peer_list = peer_list()

    for peer in peer_list:
        if peer.hostname == hostname
            log("Found peer: {}".format(peer))
            return peer
    raise GlusterError("Unable to find peer by hostname: {}".format(hostname))


def parse_peer_status(line): # -> Result<Vec<Peer>, GlusterError>
    """
        Take a peer status line and parse it into Peer objects
        line: String.  The peer status CLI output
        :return: List of Peer objects
    """
     peers = []
    regex_str = re.compile("Hostname:\s+(?P<hostname>[a-zA-Z0-9.]+)\s+Uuid:\s+(?P<uuid>\w+-\w+-\w+-\w+-\w+)\s+State:\s+(?P<state_detail>[a-zA-z ]+)\s+\((?P<state>\w+)\)")
    peer_regex = regex_str.replace("\n", "")
    for cap in peer_regex.captures_iter(line):
        hostname = cap.name("hostname")
            .ok_or(GlusterError(format!("Invalid hostname for peer: ", line)))

        uuid = cap.name("uuid")
            .ok_or(GlusterError(format!("Invalid uuid for peer: ", line)))
        uuid_parsed = Uuid::parse_str(uuid)
        state_details = cap.name("state_detail")
            .ok_or(GlusterError(format!("Invalid state for peer: ", line)))

        # Translate back into an IP address if needed
        check_for_ip = hostname.parse::<IpAddr>()

        if check_for_ip.is_err():
            # It's a hostname so lets resolve it
            match resolve_to_ip(hostname)
                Ok(ip_addr) =>
                    peers.push(Peer(
                        uuid=uuid_parsed,
                        hostname=ip_addr,
                        status=State::new(state_details))
                    continue
                Err(e) =>
                    raise GlusterError(e)
         else:
            # It's an IP address so lets use it
            peers.push(Peer(
                uuid=uuid_parsed,
                hostname=hostname,
                status=State::new(state_details))
    return Ok(peers)


def peer_status(): #  -> Result<Vec<Peer>, GlusterError>
    """
    # Runs gluster peer status and returns a Vec<Peer> representing all the peers
    # in the cluster
    # # Failures
    # Returns GlusterError if the command failed to run
    """
    arg_list = ["peer", "status"]

    output = run_command("gluster", arg_list, True, False)
    output_str = output.stdout.decode('utf8')
    # Number of Peers: 1
    # Hostname: 10.0.3.207
    # Uuid: afbd338e-881b-4557-8764-52e259885ca3
    # State: Peer in Cluster (Connected)
    #
    return parse_peer_status(output_str)


def peer_list():
    """
    List all peers including localhost
    Runs gluster pool list and returns a Vec<Peer> representing all the peers
    in the cluster
    This also returns information for the localhost as a Peer.  peer_status()
    does not
    # Failures
    Returns GlusterError if the command failed to run
    """
    peers = []
    arg_list = ["pool", "list"]

    output = run_command("gluster", arg_list, True, False)
    output_str = output.stdout.decode('utf8')

    for line in output_str.lines():
        if line.contains("State"):
            continue
         else:
            v: Vec<str> = line.split('\t').collect()
            uuid = (Uuid::parse_str(v[0]))
             hostname = v[1].trim()

            # Translate back into an IP address if needed
            check_for_ip = hostname.parse::<IpAddr>()

            if check_for_ip.is_err():
                # It's a hostname so lets resolve it
                hostname = match resolve_to_ip(hostname)
                    Ok(ip_addr) => ip_addr,
                    Err(e) =>
                        raise Err(GlusterError(e))
            log("hostname from peer list command is :?", hostname)
            peers.push(Peer(uuid=uuid, hostname=hostname, status=State::new(v[2]))
    return Ok(peers)


def peer_probe(hostname):
    # Probe a peer and prevent double probing
    # Adds a new peer to the cluster by hostname or ip address
    # # Failures
    # Returns GlusterError if the command failed to run
    current_peers = peer_list()
    for peer in current_peers:
        if peer.hostname == *hostname:
            # Bail instead of double probing
            # raise Err(format!("hostname:  is already part of the cluster", hostname))
            return 0 #Does it make sense to say this is ok?

    arg_list = ["peer", "probe", hostname]
    return process_output(run_command("gluster", arg_list, True, False))


def peer_remove(hostname, force):
    # Removes a peer from the cluster by hostname or ip address
    # # Failures
    # Returns GlusterError if the command failed to run
    arg_list = ["peer", "detach", hostname]
    if force:
        arg_list.push("force")

    return process_output(run_command("gluster", arg_list, True, False))

