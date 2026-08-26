"""
Centralized Secrets Vault
=========================
Stores and synchronizes API keys across the distributed Swarm 
using the master PostgreSQL database.

Usage (Master Node Only):
  python vault.py --set ALPACA_API_KEY --value "PK_NEW_KEY_HERE"
  python vault.py --set ALPACA_SECRET_KEY --value "NEW_SECRET_HERE"
  python vault.py --list
"""

import os
import sys
import argparse
from sqlalchemy import create_engine, Column, String, text
from sqlalchemy.orm import declarative_base, sessionmaker

# Use the exact same DB connection logic as the rest of the Swarm
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'market_cache.db')
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{db_path}")

engine = create_engine(DATABASE_URL)
Base = declarative_base()

class SystemSecret(Base):
    __tablename__ = 'system_secrets'
    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def load_secrets(overwrite=True):
    """
    Pulls all secrets from the DB and dynamically injects them into 
    the OS environment variables in RAM.
    """
    session = Session()
    try:
        secrets = session.query(SystemSecret).all()
        for secret in secrets:
            # Inject into the environment BEFORE the script asks for os.getenv()
            if overwrite or secret.key not in os.environ:
                os.environ[secret.key] = secret.value
    except Exception as e:
        print(f"⚠️ Vault Load Error: {e}")
    finally:
        session.close()

def set_secret(key, value):
    """Sets or updates a secret in the central DB."""
    session = Session()
    try:
        session.merge(SystemSecret(key=key, value=value))
        session.commit()
        print(f"✅ Successfully locked secret into Vault: {key}")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to set secret: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Distributed API Keys")
    parser.add_argument("--set", type=str, help="The key to set (e.g., ALPACA_API_KEY)")
    parser.add_argument("--value", type=str, help="The value of the key")
    parser.add_argument("--list", action="store_true", help="List all stored keys (values are hidden)")
    args = parser.parse_args()

    if args.set and args.value:
        set_secret(args.set, args.value)
    elif args.list:
        session = Session()
        secrets = session.query(SystemSecret).all()
        print("\n🔐 Stored System Secrets:")
        for s in secrets:
            # Mask the value so you don't leak it on your terminal screen
            if len(s.value) > 8:
                masked = s.value[:4] + "*" * (len(s.value) - 8) + s.value[-4:]
            else:
                masked = "****"
            print(f" - {s.key}: {masked}")
        session.close()
    else:
        parser.print_help()
