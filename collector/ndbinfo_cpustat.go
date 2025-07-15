// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `ndbinfo.cpustat`

package collector

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoCpustatQuery = `
        SELECT cp.node_id, cp.thr_no, threads.thread_name,
          SUM(cp.exec_time) * 100 /  SUM(elapsed_time) as exec_perc,
          SUM(cp.sleep_time) * 100 / SUM(elapsed_time) as sleep_perc,
          SUM(cp.send_time) * 100 / SUM(cp.elapsed_time) as send_perc,
          SUM(cp.spin_time) * 100 / SUM(cp.elapsed_time) as spin_perc
          from cpustat_1sec as cp, threads
          WHERE cp.node_id = threads.node_id AND cp.thr_no = threads.thr_no
          GROUP BY cp.node_id, threads.thread_name, cp.thr_no;
	`

var (
	ndbinfoCpustatExecPerc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "exec_percentage"),
		"Percentage of time the thread is executing",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoCpustatSleepPerc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "sleep_percentage"),
		"Percentage of time the thread is sleeping",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoCpustatSendPerc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "send_percentage"),
		"Percentage of time the thread is sending",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoCpustatSpinPerc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "spin_percentage"),
		"Percentage of time the thread is spinning",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)
)

// ScrapeNdbinfoCpustat collects for `ndbinfo.threadstat`
type ScrapeNdbinfoCpustat struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoCpustat) Name() string {
	return "ndbinfo.cpustat"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoCpustat) Help() string {
	return "Collect metrics from ndbinfo.cpustat"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoCpustat) Version() float64 {
	return 8.0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoCpustat) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	ndbinfoCpustatRows, err := db.QueryContext(ctx, ndbinfoCpustatQuery)
	if err != nil {
		return err
	}
	defer ndbinfoCpustatRows.Close()

	var (
		nodeID, threadNO                                           uint64
                execPerc, sleepPerc, sendPerc, SpinPerc                    float64
		threadName                                                 string
	)
	for ndbinfoCpustatRows.Next() {
		if err := ndbinfoCpustatRows.Scan(
			&nodeID, &threadNO, &threadName, &execPerc, &sleepPerc, &sendPerc, &spinPerc); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			ndbinfoCpustatExecPerc, prometheus.GaugeValue, execPerc,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)

		ch <- prometheus.MustNewConstMetric(
			ndbinfoCpustatSleepPerc, prometheus.GaugeValue, sleepPerc,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)

		ch <- prometheus.MustNewConstMetric(
			ndbinfoCpustatSendPerc, prometheus.GaugeValue, sendPerc,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)

		ch <- prometheus.MustNewConstMetric(
			ndbinfoCpustatSpinPerc, prometheus.GaugeValue, spinPerc,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
	}
	return nil
}
