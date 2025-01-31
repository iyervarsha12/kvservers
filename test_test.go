package kvpaxos

import (
	omnipaxos "cs651/a2-omnipaxos"
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

const PaxosElectionTimeout = 1000 * time.Millisecond

var prettyPrint bool
var logLevel int

func init() {
	flag.BoolVar(&prettyPrint, "pretty", true, "Enable pretty printing for zerolog")
	flag.IntVar(&logLevel, "loglevel", 1, "Set the log level for zerolog (-1=trace, 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=panic)")
}

func TestMain(m *testing.M) {
	// Parse the flags
	flag.Parse()

	// Set up the logger with the pretty print flag
	omnipaxos.SetupLogger(logLevel, prettyPrint)
	SetupLogger(logLevel, prettyPrint)

	// Run the tests
	os.Exit(m.Run())
}

// get/put/putappend that keep counts
func Get(cfg *config, ck *Clerk, key string) string {
	v := ck.Get(key)
	cfg.op()
	return v
}

func Put(cfg *config, ck *Clerk, key string, value string) {
	ck.Put(key, value)
	cfg.op()
}

func Append(cfg *config, ck *Clerk, key string, value string) {
	ck.Append(key, value)
	cfg.op()
}

func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals it is done
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn ncli clients and wait until they are all done
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(PaxosElectionTimeout)
	}
}

// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxomnipaxosstate is a positive number, the size of the state for OmniPaxos (i.e., log
// size) shouldn't exceed 8*maxomnipaxosstate. If maxomnipaxosstate is negative,
// snapshots shouldn't be used.
func TestBasicOneClient3A(t *testing.T) {

	title := "Test: "

	title = title + "Basic Test One Client"

	const nservers = 3
	const nclients = 1
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestBasicManyClients3A(t *testing.T) {

	title := "Test: "

	title = title + "Basic Test Many Client"

	const nservers = 3
	const nclients = nservers
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestUnreliableNetwork3A(t *testing.T) {

	title := "Test: "

	title = title + "Unreliable Test Network"

	const nservers = 3
	const nclients = 1
	const unreliable = true
	cfg := make_config(t, nservers, unreliable, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	for i := 0; i < 5; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
			// log.Printf("%d: client new append %v\n", cli, nv)
			Append(cfg, myck, key, nv)
			last = NextValue(last, nv)
			j++
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestFig5APartitioningOne3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5A Partitioning One Client"

	const nservers = 5
	const nclients = 1
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	// Partitioning

	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.disconnect(i, cfg.All())
	}

	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	S := rand.Int() % nservers

	// Loop runs until S is not the leader
	for ; S == leader1; S = rand.Int() % nservers {
	}

	var serversToConnectTo []int
	for i := 0; i < nservers; i++ {
		serversToConnectTo = append(serversToConnectTo, i)
	}
	cfg.connect(S, serversToConnectTo)

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestFig5APartitioningMany3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5A Partitioning Many Clients"

	const nservers = 5
	const nclients = 3
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 5; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	// sleep to reach some stability
	time.Sleep(PaxosElectionTimeout)

	// Partitioning

	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.disconnect(i, cfg.All())
	}

	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	// define S
	S := rand.Int() % nservers

	// Loop runs until S is not the leader
	for ; S == leader1; S = rand.Int() % nservers {
	}

	var serversToConnectTo []int
	for i := 0; i < nservers; i++ {
		serversToConnectTo = append(serversToConnectTo, i)
	}
	cfg.connect(S, serversToConnectTo)

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	basicFunc()

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands
	basicFunc()

	cfg.end()
}

func TestFig5BPartitioningOne3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5B Partitioning One Client"

	const nservers = 5
	const nclients = 1
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 10; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	basicFunc()

	// Partitioning
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.disconnect(i, cfg.All())
	}

	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	// define S
	S := rand.Int() % nservers
	for ; S == leader1; S = rand.Int() % nservers {
	}

	// connect S to everyone (but leader1)
	var serversToConnectTo []int
	for i := 0; i < nservers; i++ {
		if i != leader1 {
			serversToConnectTo = append(serversToConnectTo, i)
		}
	}
	cfg.connect(S, serversToConnectTo)

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestFig5BPartitioningMany3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5B Partitioning Many Clients"

	const nservers = 5
	const nclients = 3
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 5; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	// sleep to reach some stability
	time.Sleep(PaxosElectionTimeout)

	// Partitioning
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.disconnect(i, cfg.All())
	}

	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	// define S
	S := rand.Int() % nservers
	for ; S == leader1; S = rand.Int() % nservers {
	}

	// connect S to everyone (but leader1)
	var serversToConnectTo []int
	for i := 0; i < nservers; i++ {
		if i != leader1 {
			serversToConnectTo = append(serversToConnectTo, i)
		}
	}
	cfg.connect(S, serversToConnectTo)

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	basicFunc()

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands
	basicFunc()

	cfg.end()
}

func TestFig5CPartitioningOne3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5C Partitioning One Client"

	const nservers = 3
	const nclients = 1
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 10; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	basicFunc()

	// Partitioning
	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	// define S
	S := rand.Int() % nservers
	for ; S == leader1; S = rand.Int() % nservers {
	}

	cfg.connect(S, []int{leader1})

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands

	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)

			for i := 0; i < 50; i++ {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
				v := Get(cfg, myck, key)
				if v != last {
					log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
				}
			}
		})

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

	}

	cfg.end()
}

func TestFig5CPartitioningMany3A(t *testing.T) {
	title := "Test: "

	title = title + "Fig 5C Partitioning Many Clients"

	const nservers = 3
	const nclients = 3
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 5; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	// sleep to reach some stability
	time.Sleep(PaxosElectionTimeout)

	// Partitioning
	ok, leader1 := cfg.Leader()

	if !ok {
		log.Fatalf("No leader found!")
	}

	// define S
	S := rand.Int() % nservers
	for ; S == leader1; S = rand.Int() % nservers {
	}

	cfg.connect(S, []int{leader1})

	// log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// More commands
	basicFunc()

	// Un-Partition
	for i := 0; i < nservers; i++ {
		// disconnect everyone
		cfg.connect(i, cfg.All())
	}

	time.Sleep(PaxosElectionTimeout * 2)

	// Commands
	basicFunc()

	cfg.end()
}

func TestPersistOne3A(t *testing.T) {
	title := "Test: "

	title = title + "Persist One Client"

	const nservers = 5
	const nclients = 1
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 15; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	basicFunc()

	// Crashing

	// log.Printf("shutdown servers\n")
	for i := 0; i < nservers/2; i++ {
		cfg.ShutdownServer(i)
	}
	// Wait for a while for servers to shutdown, since
	// shutdown isn't a real crash and isn't instantaneous
	time.Sleep(PaxosElectionTimeout)
	// log.Printf("restart servers\n")
	// crash and re-start all
	for i := 0; i < nservers/2; i++ {
		cfg.StartServer(i)
	}
	cfg.ConnectAll()
	time.Sleep(PaxosElectionTimeout)

	// More commands
	basicFunc()

	cfg.end()
}

func TestPersistMany3A(t *testing.T) {
	title := "Test: "

	title = title + "Persist Many Clients"

	const nservers = 5
	const nclients = 3
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}

	// Put(cfg, myck, key, last)
	// Append(cfg, myck, key, nv)
	// v := Get(cfg, myck, key)

	basicFunc := func() {

		for i := 0; i < 3; i++ {
			// log.Printf("Iteration %v\n", i)
			go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
				j := 0
				defer func() {
					clnts[cli] <- j
				}()
				last := ""
				key := strconv.Itoa(cli)
				Put(cfg, myck, key, last)

				for i := 0; i < 15; i++ {
					if (rand.Int() % 1000) < 500 {
						nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
						// log.Printf("%d: client new append %v\n", cli, nv)
						Append(cfg, myck, key, nv)
						last = NextValue(last, nv)
						j++
					} else {
						// log.Printf("%d: client new get %v\n", cli, key)
						v := Get(cfg, myck, key)
						if v != last {
							log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
						}
					}
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			})

			// log.Printf("wait for clients\n")
			for i := 0; i < nclients; i++ {
				// log.Printf("read from clients %d\n", i)
				j := <-clnts[i]
				// if j < 10 {
				// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
				// }
				key := strconv.Itoa(i)
				// log.Printf("Check %v for client %d\n", j, i)
				v := Get(cfg, ck, key)
				checkClntAppends(t, i, v, j)
			}

		}
	}

	basicFunc()

	// Crashing
	// log.Printf("shutdown servers\n")
	for i := 0; i < nservers/2; i++ {
		cfg.ShutdownServer(i)
	}
	// Wait for a while for servers to shutdown, since
	// shutdown isn't a real crash and isn't instantaneous
	time.Sleep(PaxosElectionTimeout)
	// log.Printf("restart servers\n")
	// crash and re-start all
	for i := 0; i < nservers/2; i++ {
		cfg.StartServer(i)
	}
	cfg.ConnectAll()
	time.Sleep(PaxosElectionTimeout)

	// More commands
	basicFunc()

	cfg.end()
}
