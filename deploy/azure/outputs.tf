output "server_public_ips" {
  description = "Public IPs of FerricStore server nodes"
  value       = azurerm_public_ip.bench[*].ip_address
}

output "server_private_ips" {
  description = "Private IPs of FerricStore server nodes"
  value       = azurerm_network_interface.bench[*].private_ip_address
}

output "client_public_ip" {
  description = "Public IP of the benchmark client"
  value       = azurerm_public_ip.client.ip_address
}

output "ssh_command" {
  description = "SSH to first server node"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.bench[0].ip_address}"
}

output "ssh_client" {
  description = "SSH to benchmark client"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.client.ip_address}"
}
