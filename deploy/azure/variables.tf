variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "ferricstore-bench"
}

variable "vm_size" {
  description = "VM size — L4s_v3 has local NVMe"
  type        = string
  default     = "Standard_L4s_v3"
}

variable "node_count" {
  description = "Number of FerricStore nodes (1 for single, 3 for cluster)"
  type        = number
  default     = 1
}

variable "admin_username" {
  description = "SSH admin username"
  type        = string
  default     = "ferric"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "elixir_version" {
  description = "Elixir version to install"
  type        = string
  default     = "1.19.5"
}

variable "otp_version" {
  description = "Erlang/OTP version to install"
  type        = string
  default     = "28.3.1"
}

variable "shard_count" {
  description = "FerricStore shard count (0 = auto = schedulers_online)"
  type        = number
  default     = 0
}

variable "ferricstore_port" {
  description = "FerricStore TCP port"
  type        = number
  default     = 6379
}
