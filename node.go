package main

import (
	"bytes"
	"log"
)

//PxsStatus : status
type PxsStatus int

const (
	//PxsStatusOK : OK
	PxsStatusOK PxsStatus = 0
	//PxsStatusClusterUnavailable : Cluster is unavailable;
	PxsStatusClusterUnavailable PxsStatus = 1
	//PxsStatusNotProposerLeader : current proposer is not leader;
	PxsStatusNotProposerLeader PxsStatus = 2
	//PxsStatusNetworkIOFailure :
	PxsStatusNetworkIOFailure PxsStatus = 3
)

//INode communication.
type INode interface {
	GetID() uint32
	Start() error
	//Stop() error
	OnRecv(src uint32, data []byte)
	//send to some node with ID src
	SendTo(dst uint32, data []byte) (int, error)
}

//Node :
type Node struct {
	id     uint32
	cfg    *ClusterConfig           //proposer,acceptor,learner config
	trans  *UDPTransport            //XXX make it generic.
	bufMap map[uint32]*bytes.Buffer //incoming peer msg buffers.
	//node/cluster config
	peers  []uint32 //peers ID
	quorum uint32   //min number of acceptor to chose a proposal
	//protocol roles:
	client   *Client
	proposer *Proposer
	acceptor *Acceptor
	learner  *Learner
	//persistent states
	instanceID uint32 //globally auto incremental instance ID
	leaderID   uint32 //leader proposer ID
}

//NewNode : ctor of Node
func NewNode(id uint32) *Node {
	n := new(Node)
	n.id = id
	n.trans = NewUDPTransport(id)
	n.trans.OnRecv = n.OnRecv
	n.bufMap = make(map[uint32]*bytes.Buffer)
	//TODO recover from stable storage
	n.instanceID = 1
	return n
}

//GetID : return ID.
func (n Node) GetID() uint32 {
	return n.id
}

//NewNodeLoad : generate node from config file
func NewNodeLoad(cfgFile string) *Node {
	//new cfg
	cfg := new(ClusterConfig)
	err := cfg.LoadFromFile(cfgFile)
	if err != nil {
		log.Panic("Load config FAILED:", err)
		return nil
	}
	//new node with cfg
	node := NewNode(cfg.NodeID)
	node.cfg = cfg
	//peer list: init buffer.
	for _, id := range node.cfg.ServerList {
		node.bufMap[id] = new(bytes.Buffer)
	}
	//TODO to be chose by leader election proposal
	node.leaderID = 0 //leader is not selected
	node.quorum = uint32(len(node.cfg.AcceptorList)/2 + 1)
	return node
}

//Start - start transport server
func (n *Node) Start() error {
	//start transport
	err := n.trans.Start()
	if err != nil {
		return err
	}

	if n.cfg == nil {
		log.Panicf("empty cfg for node:%+v\n", n)
		return nil
	}

	//start proposer
	for _, v := range n.cfg.ProposerList {
		if v == n.id {
			n.proposer = NewProposer(n)
			n.proposer.Start()
		}
	}

	//start acceptor
	for _, v := range n.cfg.AcceptorList {
		if v == n.id {
			n.acceptor = NewAcceptor(n)
			n.acceptor.Start()
		}
	}

	//start learner
	for _, v := range n.cfg.LearnerList {
		if v == n.id {
			n.learner = new(Learner)
			n.learner.node = n
			n.learner.Start()
		}
	}

	//TODO start client.
	if n.id == 9 {
		n.client = new(Client)
		n.client.node = n
		n.client.Start()
	}

	return err
}

//OnRecv : on data recv from transport
func (n *Node) OnRecv(from uint32, data []byte) {
	//log.Printf("[%d]Node.OnRecv - from:%d,data:%+v\n", n.id, from, data)
	buf, ok := n.bufMap[from]
	if !ok {
		//log.Println("DROPED.")
		n.bufMap[from] = new(bytes.Buffer)
		buf = n.bufMap[from]
	}
	//decode one msg once.
	msg, hdr, rem, err := DecodeOnePxsMsg(buf, data)
	if msg == nil || hdr == nil || err != nil {
		log.Printf("[%d]Decode failed - from:%d, data:%+v, err:%s, buffer reset.\n", n.id, from, data, err)
		buf.Reset()
		return
	}
	//handle incoming msg
	switch hdr.typ {
	case PxsMsgTypeRequest: //PxsMsgType = 0x0a //0a msg: pro -> cli
		if n.proposer != nil {
			req := msg.(*PxsMsgRequest)
			sts := n.proposer.OnRecvRequest(req, from)
			if sts != 0 { // reply early
				rsp := NewPxsMsgResponse(req.hdr.iid, uint32(sts))
				bs, _ := rsp.Encode()
				n.SendTo(from, bs)
			}
		}
	case PxsMsgTypePrepare: //PxsMsgType = 0x1a //1a msg: pro -> acc
		if n.acceptor != nil {
			prp := msg.(*PxsMsgPrepare)
			n.acceptor.OnRecvPrepare(prp, from)
		}
	case PxsMsgTypePromise: //PxsMsgType = 0x1b //1b msg: acc -> pro
		if n.proposer != nil {
			pro := msg.(*PxsMsgPromise)
			n.proposer.OnRecvPromise(pro, from)
		}
	case PxsMsgTypeAccept: //PxsMsgType = 0x2a //2a msg: pro -> acc
		if n.acceptor != nil {
			acc := msg.(*PxsMsgAccept)
			n.acceptor.OnRecvAccept(acc, from)
		}
	case PxsMsgTypeAccepted: //PxsMsgType = 0x2b //2b msg: acc -> pro
		if n.proposer != nil {
			acd := msg.(*PxsMsgAccepted)
			n.proposer.OnRecvAccepted(acd, from)
		}
	case PxsMsgTypeCommit: //PxsMsgType = 0x3a //3a msg: pro -> acc
		if n.acceptor != nil {
			cmt := msg.(*PxsMsgCommit)
			n.acceptor.OnRecvCommit(cmt, from)
		}
	case PxsMsgTypeResponse: //PxsMsgType = 0x0b //0b msg: pro -> cli
		if n.client != nil {
			rsp := msg.(*PxsMsgResponse)
			sts := n.client.OnRecvResponse(rsp, from)
			log.Printf("client.OnRecvResponse - ret:%d\n", sts)
		}
	}

	if rem != 0 {
		log.Printf("Decode rem:%d, expect more.\n", rem)
	}
}

//SendTo : remote node
func (n *Node) SendTo(id uint32, data []byte) (int, error) {

	return n.trans.SendTo(id, data)
}
