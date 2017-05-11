from enum import Enum
import xml.etree.ElementTree as etree
from typing import List

from .lib import GlusterError, run_command

"""
#[test]
fn test_parse_peer_status()
    test_result =
        [Peer
                 uuid: "afbd338e-881b-4557-8764-52e259885ca3",
                 hostname: "10.0.3.207",
                 status: State.PeerInCluster,
             ,
             Peer
                 uuid: "fa3b031a-c4ef-43c5-892d-4b909bc5cd5d",
                 hostname: "10.0.3.208",
                 status: State.PeerInCluster,
             ,
             Peer
                 uuid: "5f45e89a-23c1-41dd-b0cd-fd9cf37f1520",
                 hostname: "10.0.3.209",
                 status: State.PeerInCluster,
             ]
    test_line = r#"Number of Peers: 3 Hostname: 10.0.3.207
Uuid: afbd338e-881b-4557-8764-52e259885ca3 State: Peer in Cluster (Connected)
Hostname: 10.0.3.208 Uuid: fa3b031a-c4ef-43c5-892d-4b909bc5cd5d
State: Peer in Cluster (Connected) Hostname: 10.0.3.209
Uuid: 5f45e89a-23c1-41dd-b0cd-fd9cf37f1520 State: Peer in Cluster (Connected)"#


    # Expect a 3 peer result
    result = parse_peer_status(test_line)
    println!("Result: {}".format(result))
    assert!(result.is_ok())

    result_unwrapped = result
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

    def __str__(self) -> str:
        return "{}".format(self.value)

    @staticmethod
    def from_str(s: str):
        s = s.lower()
        if s == 'connected':
            return State.Connected
        elif s == 'disconnected':
            return State.Disconnected
        elif s == 'establishing connection':
            return State.EstablishingConnection
        elif s == 'probe sent to peer':
            return State.ProbeSentToPeer
        elif s == 'probe received from peer':
            return State.ProbeReceivedFromPeer
        elif s == 'peer in cluster':
            return State.PeerInCluster
        elif s == 'accepted peer in cluster':
            return State.AcceptedPeerRequest
        elif s == "sent and received peer request":
            return State.SentAndReceivedPeerRequest
        elif s == "peer rejected":
            return State.PeerRejected
        elif s == "peer detach in progress":
            return State.PeerDetachInProgress
        elif s == "connected to peer":
            return State.ConnectedToPeer
        elif s == "peer is connected and accepted":
            return State.PeerIsConnectedAndAccepted
        elif s == "invalid state":
            return State.InvalidState
        else:
            return None


class Peer(object):
    def __init__(self, uuid, hostname: str, status: State):
        """
        A Gluster Peer.  A Peer is roughly equivalent to a server in Gluster.
        :param uuid: Unique identifier of this peer
        :param hostname:  hostname or ip address of the peer
        :param status:  current State of the peer
        """
        self.uuid = uuid
        self.hostname = hostname
        self.status = status

    def __eq__(self, other):
        return self.uuid == other.uuid

    def __str__(self):
        return "UUID: {}  Hostname: {} Status: {}".format(
            self.uuid.hyphenated(),
            self.hostname,
            self.status)


def get_peer(hostname: str):
    """
    This will query the Gluster peer list and return a Peer class for the peer
    :param hostname: String.  Name of the peer to get
    :return Peer or None in case of not found
    """
    peers = peer_list()

    for peer in peers:
        if peer.hostname == hostname:
            return peer
    return None


def parse_peer_status(output_xml: str) -> List[Peer]:
    """
    Take a peer status line and parse it into Peer objects
    :param output_xml: String.  THe peer status --xml output
    :return: List of Peer objects
    """
    peers = []
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = child.text
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        raise GlusterError(message=err_string)

    peer_tree = tree.find('peerStatus')
    for status in peer_tree:
        uuid = None
        hostname = None
        state = None
        if status.tag == 'peer':
            for peer_info in status:
                if peer_info.tag == 'uuid':
                    uuid = peer_info.text
                elif peer_info.tag == 'hostname':
                    hostname = peer_info.text
                elif peer_info.tag == 'stateStr':
                    state = State.from_str(peer_info.text)
            peers.append(Peer(uuid=uuid, hostname=hostname, status=state))

    return peers


def peer_status() -> List[Peer]:
    """
    Runs gluster peer status and returns a Vec<Peer> representing all the 
    peers in the cluster
    Returns GlusterError if the command failed to run
    :return: List of Peers
    """
    arg_list = ["peer", "status", "--xml"]

    output = run_command("gluster", arg_list, True, False)
    output_str = output.stdout.decode('utf8')
    return parse_peer_status(output_str)


def peer_list() -> List[Peer]:
    """
    List all peers including localhost
    Runs gluster pool list and returns a Vec<Peer> representing all the peers
    in the cluster
    This also returns information for the localhost as a Peer.  peer_status()
    does not
    # Failures
    Returns GlusterError if the command failed to run
    """
    arg_list = ["pool", "list", "--xml"]

    output = run_command("gluster", arg_list, True, False)
    output_xml = output.stdout.decode('utf8')

    peers = []
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = child.text
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        raise GlusterError(message=err_string)

    peer_tree = tree.find('peerStatus')
    for status in peer_tree:
        uuid = None
        hostname = None
        state = None
        if status.tag == 'peer':
            for peer_info in status:
                if peer_info.tag == 'uuid':
                    uuid = peer_info.text
                elif peer_info.tag == 'hostname':
                    hostname = peer_info.text
                elif peer_info.tag == 'stateStr':
                    state = State.from_str(peer_info.text)
            peers.append(Peer(uuid=uuid, hostname=hostname, status=state))

    return peers


def peer_probe(hostname: str) -> (int, str):
    """
    Probe a peer and prevent double probing
    Adds a new peer to the cluster by hostname or ip address
    :param hostname: String.  Add a host to the cluster
    :return:
    """
    current_peers = peer_list()
    for peer in current_peers:
        if peer.hostname == hostname:
            # Bail instead of double probing
            return 0  # Does it make sense to say this is ok?

    arg_list = ["peer", "probe", hostname]
    return run_command("gluster", arg_list, True, False)


def peer_remove(hostname: str, force: bool) -> (int, str):
    """
    Removes a peer from the cluster by hostname or ip address
    :param hostname: String.  Hostname to remove from the cluster
    :param force: bool.  Should the command be forced
    :return:
    """
    arg_list = ["peer", "detach", hostname]
    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, False)
