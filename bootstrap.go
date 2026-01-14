package worker

import (
	"context"

	internalbootstrap "github.com/publikey/runqy-worker/internal/bootstrap"
)

// Re-export types from internal/bootstrap for internal use

// BootstrapResponse contains the configuration received from runqy-server.
type BootstrapResponse = internalbootstrap.Response

// BootstrapRedisConfig holds Redis connection settings from server.
type BootstrapRedisConfig = internalbootstrap.RedisConfig

// BootstrapQueueConfig holds queue configuration from server.
type BootstrapQueueConfig = internalbootstrap.QueueConfig

// BootstrapDeploymentConfig holds code deployment settings from server.
type BootstrapDeploymentConfig = internalbootstrap.DeploymentConfig

// ProcessSupervisor manages a supervised Python FastAPI process.
type ProcessSupervisor = internalbootstrap.ProcessSupervisor

// DeploymentResult contains paths to the deployed code and virtual environment.
type DeploymentResult = internalbootstrap.DeploymentResult

// doBootstrap contacts the runqy-server and retrieves worker configuration.
func doBootstrap(ctx context.Context, config Config, logger Logger) (*BootstrapResponse, error) {
	// Convert Config to bootstrap.Config
	bsCfg := internalbootstrap.Config{
		ServerURL:           config.ServerURL,
		APIKey:              config.APIKey,
		Queue:               config.Queue,
		Version:             config.Version,
		BootstrapRetries:    config.BootstrapRetries,
		BootstrapRetryDelay: config.BootstrapRetryDelay,
		DeploymentDir:       config.DeploymentDir,
		GitSSHKey:           config.GitSSHKey,
		GitToken:            config.GitToken,
	}

	return internalbootstrap.Bootstrap(ctx, bsCfg, &loggerAdapter{logger})
}

// deployCode clones a git repository, creates a Python virtual environment,
// and installs dependencies.
func deployCode(ctx context.Context, config Config, spec BootstrapDeploymentConfig, logger Logger) (*DeploymentResult, error) {
	// Convert Config to bootstrap.Config
	bsCfg := internalbootstrap.Config{
		ServerURL:           config.ServerURL,
		APIKey:              config.APIKey,
		Queue:               config.Queue,
		Version:             config.Version,
		BootstrapRetries:    config.BootstrapRetries,
		BootstrapRetryDelay: config.BootstrapRetryDelay,
		DeploymentDir:       config.DeploymentDir,
		GitSSHKey:           config.GitSSHKey,
		GitToken:            config.GitToken,
	}

	return internalbootstrap.DeployCode(ctx, bsCfg, spec, &loggerAdapter{logger})
}

// newProcessSupervisor creates a new ProcessSupervisor.
func newProcessSupervisor(deployment *DeploymentResult, spec BootstrapDeploymentConfig, logger Logger) *ProcessSupervisor {
	return internalbootstrap.NewProcessSupervisor(deployment, spec, &loggerAdapter{logger})
}
