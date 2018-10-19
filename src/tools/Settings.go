package tools


const (
	//ResultPath = "/home/acgict/result/"
	ResultPath = "/mnt/nfs/xwen/result/"
	//ResultPath = "./"
	//NFSPath = "/home/xwen/graph/16/"
	NFSPath = "/mnt/nfs/xwen/generate_graph/16p/"

	WorkerNum = 16
	//NFSPath = "../test_data/subgraph.json"
	//PartitionPath = "../test_data/partition.json"
	//NFSPath = "/home/acgict/inputgraph/"
	WorkerOnSC = false

	LoadFromJson = false

	ConfigPath = "config.txt"
	//ConfigPath = "../test_data/config.txt"

	PatternPath = "pattern.txt"
	GraphSimulationTypeModel = 100

	RPCSendSize = 10000


	ConnPoolSize = 128
	MasterConnPoolSize = 1024
)