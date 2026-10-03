# ============================================================
#  Terraform Variables
# ============================================================

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, production)"
  type        = string
  default     = "production"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "obs-pipeline-cluster"
}

variable "cluster_version" {
  description = "Kubernetes version for EKS"
  type        = string
  default     = "1.29"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "node_instance_types" {
  description = "EC2 instance types for EKS node groups"
  type        = list(string)
  default     = ["m6i.xlarge", "m6i.2xlarge"]
}

variable "node_min_size" {
  description = "Minimum number of nodes in the default node group"
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum number of nodes in the default node group"
  type        = number
  default     = 10
}

variable "node_desired_size" {
  description = "Desired number of nodes in the default node group"
  type        = number
  default     = 4
}

variable "kafka_instance_type" {
  description = "EC2 instance types for Kafka-dedicated node group"
  type        = list(string)
  default     = ["r6i.xlarge"]
}

variable "kafka_node_count" {
  description = "Number of nodes for Kafka"
  type        = number
  default     = 3
}

variable "clickhouse_instance_type" {
  description = "EC2 instance types for ClickHouse-dedicated node group"
  type        = list(string)
  default     = ["r6i.2xlarge"]
}

variable "clickhouse_node_count" {
  description = "Number of nodes for ClickHouse"
  type        = number
  default     = 2
}

