package main

import "log"

//Actor : Client/Proposer/Acceptor/Learner
type Actor interface {
	getID() uint32
}

//Client :
type Client struct {
	node *Node
}

//Start :
func (c *Client) Start() error {
	return nil
}

//LeaderID : leader proposer ID.
const LeaderID = 1

//GetLeaderID : TODO get id of leader
func GetLeaderID() uint32 {
	return LeaderID
}

//Submit : send value to proposer, return errno and error.
func (c *Client) Submit(seq uint32, val *Value) (int, error) {
	dst := GetLeaderID()
	msg := NewPxsMsgRequest(seq, val)
	oct, err := msg.Encode()
	if err != nil {
		return -1, err
	}
	return c.node.SendTo(dst, oct)
}

//OnRecvResponse : on recv
func (c *Client) OnRecvResponse(rsp *PxsMsgResponse, from uint32) (ret int) {
	log.Printf("[%d]Client.OnRecvResponse - rsp:%+v, from:%d, ret:%d\n",
		c.node.id, rsp, from, rsp.ret)
	return 0
}

//Proposer :
type Proposer struct {
	node *Node
}

//Start :
func (p *Proposer) Start() error {
	return nil
}

//OnRecvRequest : client requests.
func (p *Proposer) OnRecvRequest(req *PxsMsgRequest, from uint32) (ret int) {
	log.Printf("[%d]Proposer.OnRecvRequest - req:%+v, from:%d\n", p.node.id, req, from)
	//check cluster status and return error code.
	ret = int(PxsStatusClusterUnavailable)
	return
}

//Acceptor :
type Acceptor struct {
	node *Node
}

//Start :
func (a *Acceptor) Start() error {
	return nil
}

//Learner :
type Learner struct {
	node *Node
}

//Start :
func (a *Learner) Start() error {
	return nil
}
