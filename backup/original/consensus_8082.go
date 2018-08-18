package main

import (
	//	"bolt"
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"
)

const (
	port = ":8082"
	seq  = 1 //temporary set to 1 in the case but it is a variable
)

var (
	//	seq int
	nailSet = map[int]int{2: 1, 4: 1}
	txSet   = map[int]int{2: 1, 4: 1}
	address = []string{":8081", ":8080"}
	step    = 0 //step in a round
)

//sequence of round

type Proposal struct {
	Tx map[int]int //tx proposed using number to present
	R  int         //step in a round
	L  int         //Ledger but using seq in the case
	I  string      //node identifier
}

func (v *Proposal) Serialize() []byte {
	var result bytes.Buffer
	encoder := gob.NewEncoder(&result)

	err := encoder.Encode(v)
	if err != nil {
		fmt.Println(err)
	}

	return result.Bytes()
}

func Deserialize(d []byte) *Proposal {
	var prop Proposal
	decoder := gob.NewDecoder(bytes.NewReader(d))
	err := decoder.Decode(&prop)
	if err != nil {
		fmt.Println(err)
	}

	return &prop

}

func main() {

	go Start()
	go CheckConsensus(2, 3)
	Listen()
}

func Start() {
	/*prop := &Proposal{txSet, step, seq, port}
	fmt.Println("start prop", prop)
	byteProp := prop.Serialize()
	fmt.Println("start byteprop len", len(byteProp))

	for _, ad := range address {
		SendData(ad, byteProp)
	}
	*/
	fmt.Println("Enter Start")
	Broadcast(txSet, address)
	fmt.Println("Successful start")

}

func Listen() {
	fmt.Println("Enter listen")
	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("ln Err:", err)
	}

	n := 1
	for {
		conn, err := ln.Accept()
		fmt.Println("link ", n)
		if err != nil {
			fmt.Println("ln err:", err)
		}

		handleConnection(conn)
		n++
		//fmt.Println("txSet", txSet)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Enter handleconnection")
	byteInfo, err := ioutil.ReadAll(conn)
	if err != nil {
		fmt.Println("ioutil Err:", err)
		return
	}

	if len(byteInfo) == 0 {
		fmt.Println("No info")
		return
	}

	prop := Deserialize(byteInfo)
	//fmt.Println("handleConnection prop:", prop)
	Update(prop)
	/*	if compare {
		Broadcast(change, address)
	}*/
	conn.Close()
}

func Update(prop *Proposal) (bool, map[int]int) {
	/*
		return the changes of the set

	*/
	fmt.Println("Enter update")
	round := prop.R
	if round == 0 {
		Broadcast(nailSet, Addr(prop.I))
	} //drop obslete info

	txs := prop.Tx
	//	ledger := prop.L
	//nodeAddr := prop.I

	//fmt.Println("receive prop:", txs)
	change := make(map[int]int)
	compare := false
	for k, _ := range txs {
		if txSet[k] == 0 {
			txSet[k] = 1
			change[k] = 1
			compare = true
			fmt.Println("txSet:", k, txSet[k])
		} else {
			txSet[k] = txSet[k] + 1
			fmt.Println("txSet:", k, txSet[k])
		}
	}

	return compare, change
	/*
		step++
		newProp := &Proposal{txSet, step, seq, port}
		fmt.Println("newProp", newProp.Tx, step)
		byteNewProp := newProp.Serialize()
		SendData(nodeAddr, byteNewProp)
	*/
}

func CheckConsensus(midThreshold int, fullThreshold int) {
	/*
		Here on purpose separate a round into two steps by timers
		step=0 send the initial proposal to others and also accept the others' initial proposals which step=0
		After that, step increases while sending msg.
		In the first phase, it will send n-1 msg, step=n-1
		In the second phase, step will increase according its initial tx
		That's why here uses timer to make consensus

	*/

	/*
		Here is after 1st phase, counting votes and changing status for those collecting majority votes from other nodes
		Then broadcast the change of status

	*/
	fmt.Println("Enter consensus")
	changeSet := make(map[int]int)
	var change bool = false
	time.Sleep(25 * time.Second)
	fmt.Println("Begin to check mid consensus")
	for tx, vote := range txSet {
		if vote == midThreshold && nailSet[tx] == 0 {
			change = true
			txSet[tx] = txSet[tx] + 1
			changeSet[tx] = 1
		}
	}

	if change {
		fmt.Println("node", port, changeSet)
		Broadcast(changeSet, address)
	}

	time.Sleep(10 * time.Second)
	fmt.Println("End of midconsensus")
	fmt.Println("vote", txSet)
	/*
		Here is after 2nd phase, counting votes for those who get full votes from the quorum
		Then broadcast the validation and record to database
		Enter the next round
	*/

	var final bool = false
	fullSet := make(map[int]int)
	var res []int //It is used temporarily when no database
	for {
		fmt.Println("Begin to check final consensum")
		for tx, vote := range txSet {
			if vote == fullThreshold {
				final = true
				fullSet[tx] = vote
				res = append(res, tx)
			}
			time.Sleep(3 * time.Second)
		}

		if final {
			//Broadcast(fullSet, address)
			fmt.Println("The final consensus:", res)
			break
		}
	}

}

func Broadcast(info map[int]int, addr []string) error {
	fmt.Println("Broadcast", seq, " round ", step, "step", txSet, "address:", addr)
	newProp := &Proposal{info, step, seq, port}
	byteNewProp := newProp.Serialize()
	for _, ad := range addr {
		err := SendData(ad, byteNewProp)
		if err != nil {
			fmt.Println("err", err)
		}
	}
	step++
	//	fmt.Println("step:", step)
	return nil
}

/*
func IntervalConditionalBroadcast() {
	for {
		Broadcast()
		time.Sleep(2 * time.Second)
	}

}*/

func SendData(addr string, data []byte) error {

	fmt.Println("Send data")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Dial err:", err)
		return err
	}

	conn.Write(data)
	conn.Close()
	return nil
}

func Addr(ad string) []string {
	var addr []string
	addr = append(addr, ad)
	return addr
}

func AdSend(addr string) []string {
	var adSend []string
	for _, ad := range address {
		if strings.Compare(ad, addr) != 0 {
			adSend = append(adSend, ad)
		}
	}
	return adSend
}
