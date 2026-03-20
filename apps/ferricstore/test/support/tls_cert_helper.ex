defmodule Ferricstore.Test.TlsCertHelper do
  @moduledoc """
  Generates self-signed TLS certificates at runtime for integration tests.

  Uses Erlang/OTP's `:public_key` module to create an RSA key pair and a
  self-signed X.509 certificate without any external file dependencies. The
  certificate and key are written as PEM files to a specified directory and
  should be cleaned up by the caller.

  RSA-2048 is used instead of EC because OTP's `pkix_test_root_cert/2`
  defaults to a small EC curve (sect163k1) that TLS 1.3 rejects.

  ## Usage

      {cert_path, key_path} = TlsCertHelper.generate_self_signed(tmp_dir)
      # ... use cert_path and key_path for TLS listener ...
      File.rm(cert_path)
      File.rm(key_path)
  """

  @doc """
  Generates a self-signed certificate and RSA private key, writing them as
  PEM files into `dir`.

  Returns `{cert_path, key_path}` where both are absolute paths to PEM files.

  The certificate is created using `:public_key.pkix_test_root_cert/2` with
  an explicitly-generated RSA-2048 key. This ensures compatibility with both
  TLS 1.2 and TLS 1.3.

  ## Parameters

    - `dir` - directory to write the PEM files into (must exist)
    - `opts` - keyword options:
      - `:cn` - Common Name for the certificate (default: `"localhost"`)
  """
  @spec generate_self_signed(binary(), keyword()) :: {binary(), binary()}
  def generate_self_signed(dir, opts \\ []) do
    cn = Keyword.get(opts, :cn, "localhost")

    # Generate an RSA-2048 key explicitly -- OTP's default EC curve (sect163k1)
    # is too small for TLS 1.3 which requires at least P-256.
    rsa_key = :public_key.generate_key({:rsa, 2048, 65537})

    # pkix_test_root_cert/2 returns %{cert: der_binary, key: key_record}
    %{cert: cert_der, key: key} = :public_key.pkix_test_root_cert(cn, key: rsa_key)

    # Encode certificate to PEM
    cert_pem = :public_key.pem_encode([{:Certificate, cert_der, :not_encrypted}])

    # Encode RSA private key to PEM
    key_der = :public_key.der_encode(:RSAPrivateKey, key)
    key_pem = :public_key.pem_encode([{:RSAPrivateKey, key_der, :not_encrypted}])

    cert_path = Path.join(dir, "test_cert.pem")
    key_path = Path.join(dir, "test_key.pem")

    File.write!(cert_path, cert_pem)
    File.write!(key_path, key_pem)

    {cert_path, key_path}
  end
end
