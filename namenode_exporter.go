package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wyukawa/hadoop_exporter/consts"
	"github.com/wyukawa/hadoop_exporter/structs"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var NameNodeLabel = []string{consts.ClusterLabel, consts.HostNameLabel}

type NameNodeExportOpt struct {
	PromNamespace string
	Hostname      string
	JmxUrl        string
	cluster       string
}

type NameNodeExporter struct {
	PromNamespace string
	Hostname      string
	JmxUrl        string
	cluster       string

	TotalLoad *prometheus.Desc //当前连接数

	MissingBlocks *prometheus.Desc
	CorruptBlocks *prometheus.Desc
	ExcessBlocks  *prometheus.Desc
	BlocksTotal   *prometheus.Desc

	FilesTotal *prometheus.Desc

	ExpiredHeartbeats  *prometheus.Desc
	LastCheckpointTime *prometheus.Desc

	CapacityTotal      *prometheus.Desc
	CapacityUsed       *prometheus.Desc
	CapacityRemaining  *prometheus.Desc
	CapacityUsedNonDFS *prometheus.Desc

	StaleDataNodes             *prometheus.Desc
	NumLiveDataNodes           *prometheus.Desc
	NumDeadDataNodes           *prometheus.Desc
	VolumeFailuresTotal        *prometheus.Desc
	EstimatedCapacityLostTotal *prometheus.Desc

	HAState *prometheus.Desc
	//FSState *prometheus.Desc

	GcCount      *prometheus.Desc
	GcTimeMillis *prometheus.Desc

	MemNonHeapMaxM  *prometheus.Desc
	MemNonHeapUsedM *prometheus.Desc
	MemHeapMaxM     *prometheus.Desc
	MemHeapUsedM    *prometheus.Desc
	MemMaxM         *prometheus.Desc
}

func NewNameNodeExporter(opt *NameNodeExportOpt) *NameNodeExporter {
	descNamePrefix := opt.PromNamespace + "_"
	return &NameNodeExporter{
		PromNamespace: opt.PromNamespace,
		Hostname:      opt.Hostname,
		JmxUrl:        opt.JmxUrl,
		cluster:       opt.cluster,
		TotalLoad: prometheus.NewDesc(
			descNamePrefix+"TotalLoad", "Current number of connections",
			NameNodeLabel, nil),

		MissingBlocks: prometheus.NewDesc(
			descNamePrefix+"MissingBlocks", "Current number of missing blocks",
			NameNodeLabel, nil),
		CorruptBlocks: prometheus.NewDesc(
			descNamePrefix+"CorruptBlocks", "Current number of blocks with corrupt replicas",
			NameNodeLabel, nil),
		ExcessBlocks: prometheus.NewDesc(
			descNamePrefix+"ExcessBlocks", "Current number of excess blocks",
			NameNodeLabel, nil),
		BlocksTotal: prometheus.NewDesc(
			descNamePrefix+"BlocksTotal", "Current number of allocated blocks in the system",
			NameNodeLabel, nil),
		FilesTotal: prometheus.NewDesc(
			descNamePrefix+"FilesTotal", "Current number of files and directories",
			NameNodeLabel, nil),

		ExpiredHeartbeats: prometheus.NewDesc(
			descNamePrefix+"ExpiredHeartbeats", "Total number of expired heartbeats",
			NameNodeLabel, nil),
		LastCheckpointTime: prometheus.NewDesc(
			descNamePrefix+"LastCheckpointTime", "\tTime in milliseconds since epoch of last checkpoint",
			NameNodeLabel, nil),

		CapacityTotal: prometheus.NewDesc(
			descNamePrefix+"CapacityTotal", "Current raw capacity of DataNodes in bytes",
			NameNodeLabel, nil),
		CapacityUsed: prometheus.NewDesc(
			descNamePrefix+"CapacityUsed", "Current used capacity across all DataNodes in bytes",
			NameNodeLabel, nil),
		CapacityRemaining: prometheus.NewDesc(
			descNamePrefix+"CapacityRemaining", "Current remaining capacity in bytes",
			NameNodeLabel, nil),
		CapacityUsedNonDFS: prometheus.NewDesc(
			descNamePrefix+"CapacityUsedNonDFS", "Current space used by DataNodes for non DFS purposes in bytes",
			NameNodeLabel, nil),

		StaleDataNodes: prometheus.NewDesc(
			descNamePrefix+"StaleDataNodes", "Current number of DataNodes marked stale due to delayed heartbeat",
			NameNodeLabel, nil),
		NumLiveDataNodes: prometheus.NewDesc(
			descNamePrefix+"NumLiveDataNodes", "Number of datanodes which are currently live",
			NameNodeLabel, nil),
		NumDeadDataNodes: prometheus.NewDesc(
			descNamePrefix+"NumDeadDataNodes", "Number of datanodes which are currently dead",
			NameNodeLabel, nil),

		VolumeFailuresTotal: prometheus.NewDesc(
			descNamePrefix+"VolumeFailuresTotal", "Total number of volume failures across all Datanodes",
			NameNodeLabel, nil),
		EstimatedCapacityLostTotal: prometheus.NewDesc(
			descNamePrefix+"EstimatedCapacityLostTotal", "An estimate of the total capacity lost due to volume failures",
			NameNodeLabel, nil),

		HAState: prometheus.NewDesc(
			descNamePrefix+"HAState", "(HA-only) Current state of the NameNode: initializing or active or standby or stopping state",
			NameNodeLabel, nil),
		//FSState: prometheus.NewDesc(
		//	descNamePrefix+"FSState", "Current state of the file system: Safemode or Operational",
		//	NameNodeLabel, nil),

		GcCount: prometheus.NewDesc(
			descNamePrefix+"GcCount", "Total GC count",
			NameNodeLabel, nil),
		GcTimeMillis: prometheus.NewDesc(
			descNamePrefix+"GcTimeMillis", "Total GC time in msec",
			NameNodeLabel, nil),

		MemNonHeapMaxM: prometheus.NewDesc(
			descNamePrefix+"MemNonHeapMaxM", "Max non-heap memory size in MB",
			NameNodeLabel, nil),
		MemNonHeapUsedM: prometheus.NewDesc(
			descNamePrefix+"MemNonHeapUsedM", "Current non-heap memory used in MB",
			NameNodeLabel, nil),
		MemHeapMaxM: prometheus.NewDesc(
			descNamePrefix+"MemHeapMaxM", "Max heap memory size in MB",
			NameNodeLabel, nil),
		MemHeapUsedM: prometheus.NewDesc(
			descNamePrefix+"MemHeapUsedM", "Current heap memory used in MB",
			NameNodeLabel, nil),
		MemMaxM: prometheus.NewDesc(
			descNamePrefix+"MemMaxM", "Max memory size in MB",
			NameNodeLabel, nil),
	}
}

// Describe implements the prometheus.Collector interface.
func (e *NameNodeExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.TotalLoad
	ch <- e.MissingBlocks
	ch <- e.CorruptBlocks
	ch <- e.ExcessBlocks
	ch <- e.BlocksTotal
	ch <- e.FilesTotal
	ch <- e.ExpiredHeartbeats
	ch <- e.LastCheckpointTime
	ch <- e.CapacityUsed
	ch <- e.CapacityTotal
	ch <- e.CapacityRemaining
	ch <- e.StaleDataNodes
	ch <- e.NumLiveDataNodes
	ch <- e.NumDeadDataNodes
	ch <- e.VolumeFailuresTotal
	ch <- e.EstimatedCapacityLostTotal
	ch <- e.HAState
	//ch <- e.FSState
	ch <- e.GcCount
	ch <- e.GcTimeMillis
	ch <- e.MemMaxM
	ch <- e.MemNonHeapMaxM
	ch <- e.MemNonHeapUsedM
	ch <- e.MemHeapMaxM
	ch <- e.MemHeapUsedM
}

// Collect implements the prometheus.Collector interface.
func (e *NameNodeExporter) Collect(ch chan<- prometheus.Metric) {
	resp, err := http.Get(e.JmxUrl)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	//var f map[string]structs.JmxBean
	bean := structs.JmxBean{}
	err = json.Unmarshal(data, &bean)
	if err != nil {
		fmt.Println(err)
	}
	// {"beans":[{"name":"Hadoop:service=NameNode,name=FSNamesystem", ...}, {"name":"java.lang:type=MemoryPool,name=Code Cache", ...}, ...]}
	//var bean = f["beans"]
	for _, DataMap := range bean.Beans {
		if DataMap["name"] == consts.NNFSMetrics {
			ch <- prometheus.MustNewConstMetric(e.TotalLoad, prometheus.GaugeValue, DataMap["TotalLoad"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MissingBlocks, prometheus.GaugeValue, DataMap["MissingBlocks"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.CorruptBlocks, prometheus.GaugeValue, DataMap["CorruptBlocks"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.ExcessBlocks, prometheus.GaugeValue, DataMap["ExcessBlocks"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.BlocksTotal, prometheus.GaugeValue, DataMap["BlocksTotal"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.FilesTotal, prometheus.GaugeValue, DataMap["FilesTotal"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.LastCheckpointTime, prometheus.GaugeValue, DataMap["LastCheckpointTime"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.CapacityTotal, prometheus.GaugeValue, DataMap["CapacityTotal"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.CapacityUsed, prometheus.GaugeValue, DataMap["CapacityUsed"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.CapacityRemaining, prometheus.GaugeValue, DataMap["CapacityRemaining"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.CapacityUsedNonDFS, prometheus.GaugeValue, DataMap["CapacityUsedNonDFS"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.StaleDataNodes, prometheus.GaugeValue, DataMap["StaleDataNodes"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.NumLiveDataNodes, prometheus.GaugeValue, DataMap["NumLiveDataNodes"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.NumDeadDataNodes, prometheus.GaugeValue, DataMap["NumDeadDataNodes"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.VolumeFailuresTotal, prometheus.GaugeValue, DataMap["VolumeFailuresTotal"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.EstimatedCapacityLostTotal, prometheus.GaugeValue, DataMap["EstimatedCapacityLostTotal"].(float64), e.cluster, e.Hostname)
			switch strings.ToLower(DataMap["tag.HAState"].(string)) {
			case "stopping":
				ch <- prometheus.MustNewConstMetric(e.HAState, prometheus.GaugeValue, 0, e.cluster, e.Hostname)
			case "active":
				ch <- prometheus.MustNewConstMetric(e.HAState, prometheus.GaugeValue, 1, e.cluster, e.Hostname)
			case "initializing":
				ch <- prometheus.MustNewConstMetric(e.HAState, prometheus.GaugeValue, 3, e.cluster, e.Hostname)
			case "standby":
				ch <- prometheus.MustNewConstMetric(e.HAState, prometheus.GaugeValue, 4, e.cluster, e.Hostname)
			default:
				ch <- prometheus.MustNewConstMetric(e.HAState, prometheus.GaugeValue, -1, e.cluster, e.Hostname)
			}
		}

		if DataMap["name"] == consts.NNJVMMetrics {
			ch <- prometheus.MustNewConstMetric(e.GcCount, prometheus.GaugeValue, DataMap["GcCount"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.GcTimeMillis, prometheus.GaugeValue, DataMap["GcTimeMillis"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MemMaxM, prometheus.GaugeValue, DataMap["MemMaxM"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MemHeapUsedM, prometheus.GaugeValue, DataMap["MemHeapUsedM"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MemHeapMaxM, prometheus.GaugeValue, DataMap["MemHeapMaxM"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MemNonHeapUsedM, prometheus.GaugeValue, DataMap["MemNonHeapUsedM"].(float64), e.cluster, e.Hostname)
			ch <- prometheus.MustNewConstMetric(e.MemNonHeapMaxM, prometheus.GaugeValue, DataMap["MemNonHeapMaxM"].(float64), e.cluster, e.Hostname)
		}

	}

}

func main() {
	NameNodeNamespace := "namenode"
	listenAddress := flag.String("web.listen-address", "localhost:9070", "Address on which to expose metrics and web interface.")
	metricsPath := flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	namenodeJmxUrl := flag.String("namenode.jmx.url", "http://82.157.57.170:50070/jmx", "Hadoop JMX URL.")
	flag.Parse()
	opt := NameNodeExportOpt{
		PromNamespace: NameNodeNamespace, Hostname: "hdfs1", JmxUrl: *namenodeJmxUrl, cluster: "testcluster",
	}
	exporter := NewNameNodeExporter(&opt)
	fmt.Println(exporter.PromNamespace)
	prometheus.MustRegister(exporter)
	fmt.Println("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>NameNode Exporter</title></head>
		<body>
		<h1>NameNode Exporter</h1>
		<p><a href="/metrics">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
