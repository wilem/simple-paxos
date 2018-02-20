package main

import (
	"log"
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
	id    uint32
	cfg   *ClusterConfig //proposer,acceptor,learner config
	trans *UDPTransport  //XXX make it generic.
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

	//XXX start client.
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

}

//SendTo : remote node
func (n *Node) SendTo(id uint32, data []byte) (int, error) {

	return n.trans.SendTo(id, data)
}
