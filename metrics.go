package worker

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// SystemMetrics holds system resource metrics collected on each heartbeat.
type SystemMetrics struct {
	CPUPercent       float64      `json:"cpu_percent"`
	MemoryUsedBytes  uint64       `json:"memory_used_bytes"`
	MemoryTotalBytes uint64       `json:"memory_total_bytes"`
	GPUs             []GPUMetrics `json:"gpus,omitempty"`
	CollectedAt      int64        `json:"collected_at"`
}

// GPUMetrics holds metrics for a single NVIDIA GPU.
type GPUMetrics struct {
	Index              int     `json:"index"`
	Name               string  `json:"name"`
	UtilizationPercent float64 `json:"utilization_percent"`
	MemoryUsedMB       uint64  `json:"memory_used_mb"`
	MemoryTotalMB      uint64  `json:"memory_total_mb"`
	TemperatureC       int     `json:"temperature_c"`
}

// collectMetrics gathers CPU, memory, and GPU metrics.
func collectMetrics() (*SystemMetrics, error) {
	m := &SystemMetrics{
		CollectedAt: time.Now().Unix(),
	}

	// CPU percent (average across all cores, 0 interval = since last call)
	cpuPercents, err := cpu.Percent(0, false)
	if err == nil && len(cpuPercents) > 0 {
		m.CPUPercent = cpuPercents[0]
	}

	// Memory
	vmem, err := mem.VirtualMemory()
	if err == nil {
		m.MemoryUsedBytes = vmem.Used
		m.MemoryTotalBytes = vmem.Total
	}

	// GPU (Linux only, nvidia-smi)
	if runtime.GOOS == "linux" {
		if gpus, err := collectGPUMetrics(); err == nil {
			m.GPUs = gpus
		}
	}

	return m, nil
}

// collectGPUMetrics runs nvidia-smi to get GPU metrics.
func collectGPUMetrics() ([]GPUMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
		"--format=csv,noheader,nounits",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi: %w", err)
	}

	var gpus []GPUMetrics
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ", ")
		if len(parts) < 6 {
			continue
		}

		idx, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
		util, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		memUsed, _ := strconv.ParseUint(strings.TrimSpace(parts[3]), 10, 64)
		memTotal, _ := strconv.ParseUint(strings.TrimSpace(parts[4]), 10, 64)
		temp, _ := strconv.Atoi(strings.TrimSpace(parts[5]))

		gpus = append(gpus, GPUMetrics{
			Index:              idx,
			Name:               strings.TrimSpace(parts[1]),
			UtilizationPercent: util,
			MemoryUsedMB:       memUsed,
			MemoryTotalMB:      memTotal,
			TemperatureC:       temp,
		})
	}

	return gpus, nil
}
