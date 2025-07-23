import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

PRIVATE_KEY_PATH = "claves/dqc_svc_rsa_key.p8"
USUARIO = "dqc_svc"
CUENTA = "tpqlguw-kva22319"
ROL = "dqc_role"

def cargar_clave_privada(path):
    with open(path, "rb") as key_file:
        return serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        ).private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

def conectar_snowflake(usuario, cuenta, private_key, rol):
    return snowflake.connector.connect(
        user=usuario,
        account=cuenta,
        private_key=private_key,
        role=rol,
        authenticator="snowflake"
    )
