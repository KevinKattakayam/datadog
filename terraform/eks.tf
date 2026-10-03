# ============================================================
#  VPC — Networking Foundation
# ============================================================

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.5"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = false
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Tags required for EKS
  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"           = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

# ============================================================
#  EKS Cluster
# ============================================================

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  # Cluster add-ons
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }

  # ── Node Groups ─────────────────────────────────────────────

  eks_managed_node_groups = {
    # General workloads (ingestor, processor, monitoring)
    general = {
      name           = "general"
      instance_types = var.node_instance_types
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size

      labels = {
        workload = "general"
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 100
            volume_type = "gp3"
            iops        = 3000
            throughput   = 125
            encrypted   = true
          }
        }
      }
    }

    # Kafka-dedicated nodes (I/O optimized)
    kafka = {
      name           = "kafka"
      instance_types = var.kafka_instance_type
      min_size       = var.kafka_node_count
      max_size       = var.kafka_node_count
      desired_size   = var.kafka_node_count

      labels = {
        workload = "kafka"
      }

      taints = [{
        key    = "workload"
        value  = "kafka"
        effect = "NO_SCHEDULE"
      }]

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 500
            volume_type = "gp3"
            iops        = 6000
            throughput   = 250
            encrypted   = true
          }
        }
      }
    }

    # ClickHouse-dedicated nodes (memory + storage optimized)
    clickhouse = {
      name           = "clickhouse"
      instance_types = var.clickhouse_instance_type
      min_size       = var.clickhouse_node_count
      max_size       = var.clickhouse_node_count
      desired_size   = var.clickhouse_node_count

      labels = {
        workload = "clickhouse"
      }

      taints = [{
        key    = "workload"
        value  = "clickhouse"
        effect = "NO_SCHEDULE"
      }]

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 1000
            volume_type = "gp3"
            iops        = 10000
            throughput   = 500
            encrypted   = true
          }
        }
      }
    }
  }
}

# ============================================================
#  IAM Roles for Service Accounts (IRSA)
# ============================================================

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.34"

  role_name = "${var.cluster_name}-ebs-csi"

  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}

# ============================================================
#  Storage Classes
# ============================================================

resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner = "ebs.csi.aws.com"
  reclaim_policy      = "Retain"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    type      = "gp3"
    iops      = "3000"
    throughput = "125"
    encrypted = "true"
  }
}
