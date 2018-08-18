package main

import (
	//	"bolt"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	port          = ":8080"
	commandLength = 20
	netSize       = 4
)

var (
	//	seq int
	nailSet          = map[int]int{1: 1, 2: 1, 3: 1}
	seq              = 1
	oldSeq           int
	txSet            = map[int]int{1: 1, 2: 1, 3: 1}
	address          = []string{":8081", ":8082", ":8083"}
	step             = 0 //step in a round
	majority         []int
	midMux           sync.Mutex
	finalConsensus   []int
	finalVote        = 1
	alreadyReceStart []string
)

//sequence of round

type Proposal struct {
	Tx map[int]int //tx proposed using number to present
	R  int         //step in a round
	L  int         //Ledger but using seq in the case
	I  string      //node identifier
}

func Reset() {
	txSet = map[int]int{1: 1, 2: 1, 3: 1} //it will be a function laterly
	step = 0
	majority = []int{}
	finalConsensus = []int{}
	finalVote = 1
	alreadyReceStart = []string{}
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
	if seq == 1 {
		oldSeq = seq
	}

	SendStart() //The 1st time setup

	/*
		All nodes setup and begin regular round's setup after the 1st round
	*/

	for {
		fmt.Println("oldSeq", "seq", oldSeq, seq)
		if seq > oldSeq {
			oldSeq = seq
			SendStart()
		}
		time.Sleep(2 * time.Second)
	}

}

func SendStart() {
	fmt.Println("Enter Start")
	cmd := "start"
	Broadcast(nailSet, address, cmd)
	//fmt.Println("Successful start")
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
	}
}

func handleConnection(conn net.Conn) {
	byteInfo, err := ioutil.ReadAll(conn)
	if err != nil {
		fmt.Println("ioutil Err:", err)
		return
	}

	if len(byteInfo) == 0 {
		fmt.Println("No info")
		return
	}

	byteStdCmd := byteInfo[:commandLength]
	cmd := Byte2Command(byteStdCmd)

	byteProp := byteInfo[commandLength:]
	prop := Deserialize(byteProp)

	switch cmd {
	case "start":
		fmt.Println("Enter handleconnection start from ", prop.I)
		if AlreadyReceStart(prop.I) {
			fmt.Println("already received", alreadyReceStart)
			return
		} else {
			alreadyReceStart = append(alreadyReceStart, prop.I)
			Restart(prop.I)
			Update(prop)
			if len(alreadyReceStart) > (netSize - 2) {
				CheckMidConsensus(netSize - 1)
			}
		}
	case "mid":
		fmt.Println("Enter handleconnection mid from ", prop.I)
		Update(prop)
		CheckFinalConsensus(netSize)
	case "final":
		fmt.Println("Enter handleconnection final from ", prop.I)
		//fmt.Println("Counting the vote of final and Write to database")
		AddFinalVote(netSize, prop.Tx)
	case "restart":
		fmt.Println("Enter handleconnection restart from ", prop.I)
		if AlreadyReceStart(prop.I) {
			fmt.Println("already received", alreadyReceStart)
			return
		} else {
			alreadyReceStart = append(alreadyReceStart, prop.I)
			Update(prop)
			if len(alreadyReceStart) > (netSize - 2) {
				CheckMidConsensus(netSize - 1)
			}
		}
	default:
		fmt.Println("Now such command!")
	}

	conn.Close()
}

//receive start msg and respond with restart msg
func Restart(addr string) {
	cmd := "restart"
	Broadcast(nailSet, Addr(addr), cmd)
}

func Update(prop *Proposal) {
	/*
		return the changes of the set

	*/
	//fmt.Println("Enter update")
	/*
	   //simplify the function only doing the update job and
	   move the communication action to another function

	   round := prop.R
	   	if round == 0 && reSend {
	   		cmd := "restart"
	   		Broadcast(nailSet, Addr(prop.I), cmd)
	   	} //drop obslete info
	*/

	txs := prop.Tx

	for k, _ := range txs {
		if txSet[k] == 0 {
			txSet[k] = 1
			//	fmt.Println("txSet:", k, txSet[k])
		} else {
			txSet[k] = txSet[k] + 1
			//	fmt.Println("txSet:", k, txSet[k])
		}
	}

	//fmt.Println("NailSet:", nailSet)
	//fmt.Println("txSet:", txSet)
	/*
		step++
		newProp := &Proposal{txSet, step, seq, port}
		fmt.Println("newProp", newProp.Tx, step)
		byteNewProp := newProp.Serialize()
		SendData(nodeAddr, byteNewProp)
	*/
}

func CheckMidConsensus(midThreshold int) {
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
	fmt.Println("Enter mid consensus")
	changeSet := make(map[int]int)
	var change bool = false
	//time.Sleep(25 * time.Second)
	for tx, vote := range txSet {
		if vote == midThreshold || vote > midThreshold {
			majority = append(majority, tx)
			//fmt.Println("midconsensus majority", majority, tx)
			if nailSet[tx] == 0 {
				change = true
				txSet[tx] = txSet[tx] + 1
				changeSet[tx] = 1
			}
		}
	}

	if change {
		fmt.Println("node", port, changeSet)
		cmd := "mid"
		Broadcast(changeSet, address, cmd)
	}

	fmt.Println("End of midconsensus")
	//fmt.Println("vote", txSet)
}

/*//abandon function
func LoopMid(midThreshold int) {
	for {
		midMux.Lock()
		if step < 2 {
			midMux.Unlock()
			continue
		}
		midMux.Unlock()
		CheckMidConsensus(midThreshold)
	}
}
*/

func CheckFinalConsensus(fullThreshold int) {
	/*
		Here is after 2nd phase, counting votes for those who get full votes from the quorum
		Then broadcast the validation and record to database
		Enter the next round
	*/

	//var final bool = false
	fullSet := make(map[int]int)
	var res []int //It is used temporarily when no database

	fmt.Println("Begin to check final consensum")
	for tx, vote := range txSet {
		if vote == fullThreshold {
			//final = true
			fullSet[tx] = vote
			res = append(res, tx)
		}
		//	time.Sleep(3 * time.Second)
	}

	//if final {
	//fmt.Println(len(res), len(majority))
	if len(res) == len(majority) {
		cmd := "final"
		Broadcast(fullSet, address, cmd) //it should use another command
		finalConsensus = OrderTx(res)
		fmt.Println("The final consensus:", finalConsensus)
	}

}

func CheckValidation(rece map[int]int) error {
	res := TakeTx(rece)
	if len(res) != len(finalConsensus) {
		fmt.Println("res and finalConsensus", res, finalConsensus)
		err := errors.New("not enough votes!")
		fmt.Println(err)
		return err
	}

	for i := 0; i < len(res); i++ {
		//fmt.Println(res[i], finalConsensus[i])
		if res[i] != finalConsensus[i] {
			err := errors.New("consensus is not the same!")
			fmt.Println(err)
			return err
		}
	}

	return nil

}

func AddFinalVote(thresholdV int, rece map[int]int) error {
	err := CheckValidation(rece)
	if err != nil {
		fmt.Println(err)
		return err
	}

	finalVote = finalVote + 1
	//fmt.Println("finalVote:", finalVote)

	if finalVote == thresholdV {
		fmt.Println("The round ", seq, " write database", finalConsensus)
		seq = seq + 1
		Reset()
	}

	return nil

}

func Broadcast(info map[int]int, addr []string, cmd string) error {
	fmt.Println("Broadcast", cmd, seq, " round ", step, "step", info, "address:", addr)
	newProp := &Proposal{info, step, seq, port}
	byteNewProp := newProp.Serialize()

	content := Command2Byte(cmd)
	for i := 0; i < len(byteNewProp); i++ {
		content = append(content, byteNewProp[i])
	}

	var isErrExist bool = false
	for _, ad := range addr {
		err := SendData(ad, content)
		if err != nil {
			isErrExist = true
			fmt.Println("err", err)
			continue
		}
		step++
	}

	if isErrExist {
		err := errors.New("Send data error exists!")
		fmt.Println(err)
		return err
	}

	//fmt.Println("successfully send!", step)
	return nil
}

func SendData(addr string, data []byte) error {

	//fmt.Println("Send data")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Dial err:", err)
		return err
	}

	conn.Write(data)
	conn.Close()
	return nil
}

//used in Broadcast if the format of address is string
func Addr(ad string) []string {
	var addr []string
	addr = append(addr, ad)
	return addr
}

//in order to send all ULN except the one sending the msg
func AdSend(addr string) []string {
	var adSend []string
	for _, ad := range address {
		if strings.Compare(ad, addr) != 0 {
			adSend = append(adSend, ad)
		}
	}
	return adSend
}

func AlreadyReceStart(addr string) bool {
	var alreadyRece bool = false

	if len(alreadyReceStart) == 0 {
		return alreadyRece
	}

	for _, ad := range alreadyReceStart {
		if strings.Compare(addr, ad) == 0 {
			alreadyRece = true
			return alreadyRece
		}
	}

	return alreadyRece

}

/*
	drop those address already send start msg
	The input is alreadySend

func StillSend(addr []string) []string {
	var stillSendAd []string
	midAd := address

	for _, ad1 := range addr {
		for index, ad2 := range address {
			if ad1 == ad2 {
				midAd[index] = ""
			}
		}
	}

	for i := 0; i < len(midAd); i++ {

		if len(midAd[i]) != 0 {
			stillSendAd = append(stillSendAd, midAd[i])
		}
	}

	return stillSendAd
}
*/

func Command2Byte(cmd string) []byte {
	byteStdCmd := make([]byte, commandLength)
	byteCmd := []byte(cmd)

	for i := 0; i < len(byteCmd); i++ {
		byteStdCmd[i] = byteCmd[i]
	}

	return byteStdCmd

}

func Byte2Command(byteStdCmd []byte) string {
	if len(byteStdCmd) == 0 {
		fmt.Println("No data!")
		return ""
	}

	var trimByteCmd []byte
	for _, b := range byteStdCmd {
		if b != 0x0 {
			trimByteCmd = append(trimByteCmd, b)
		}
	}

	cmd := string(trimByteCmd)
	return cmd

}

func TakeTx(vote map[int]int) []int {
	var finalTx []int
	for tx, _ := range vote {
		finalTx = append(finalTx, tx)
	}

	finalTx = OrderTx(finalTx)
	return finalTx

}

func OrderTx(finalTx []int) []int {
	var change int = 1

	for i := 0; i < (len(finalTx)-1) && change != 0; i++ {
		change = 0
		for j := 0; j < len(finalTx)-1-i; j++ {
			if finalTx[j] > finalTx[j+1] {
				tx := finalTx[j]
				finalTx[j] = finalTx[j+1]
				finalTx[j+1] = tx
				change = 1
			}
		}

	}
	fmt.Println("orderedTx: ", finalTx)
	return finalTx
}
