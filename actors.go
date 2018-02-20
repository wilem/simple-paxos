package main

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

//Proposer :
type Proposer struct {
	node *Node
}

//Start :
func (p *Proposer) Start() error {
	return nil
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
