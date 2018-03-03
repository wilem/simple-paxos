package main

import (
	"bytes"
	"log"
)

//PxsStatus : status
type PxsStatus int

const (
	//StatusOK : OK
	PxsStatusOK PxsStatus = 0
	//ClusterUnavailable : Cluster is unavailable.
	PxsStatusClusterUnavailable PxsStatus = 1
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
	peers []uint32 //peers ID
	//protocol roles:
	client   *Client
	proposer *Proposer
	acceptor *Acceptor
	learner  *Learner
}

//NewNode : ctor of Node
func NewNode(id uint32) *Node {
	n := new(Node)
	n.id = id
	n.trans = NewUDPTransport(id)
	n.trans.OnRecv = n.OnRecv
	n.bufMap = make(map[uint32]*bytes.Buffer)
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
			n.proposer = new(Proposer)
			n.proposer.node = n
			n.proposer.Start()
		}
	}

	//start acceptor
	for _, v := range n.cfg.AcceptorList {
		if v == n.id {
			n.acceptor = new(Acceptor)
			n.acceptor.node = n
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
	log.Printf("[%d]Node.OnRecv - from:%d,data:%+v\n", n.id, from, data)
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
	case PxsMsgTypePromise: //PxsMsgType = 0x1b //1b msg: acc -> pro
	case PxsMsgTypeAccept: //PxsMsgType = 0x2a //2a msg: pro -> acc
	case PxsMsgTypeAccepted: //PxsMsgType = 0x2b //2b msg: acc -> pro
	case PxsMsgTypeCommit: //PxsMsgType = 0x3a //3a msg: pro -> acc
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
