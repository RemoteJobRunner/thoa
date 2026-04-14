"""
Helper script: run with backend's venv python from the backend directory.
Creates test schema, a user, and a real API key. Prints the private key to stdout.

Usage:
    cd ~/app/backend
    DATABASE_HOST=localhost ... python ~/thoa/tests/setup_test_db.py
"""
import asyncio
import sys
import os


async def main():
    from tests.fixtures.test_settings import configure_test_settings
    configure_test_settings()

    from db.session import create_engine, create_sessionmaker
    from db.base import Base
    from db.models.models import UserModel, ApiKeyModel, AccountStatusEnum
    from core.auth.crypto import generate_api_key
    from datetime import datetime

    engine = create_engine()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    sessionmaker = create_sessionmaker(engine)

    async with sessionmaker() as db:
        user = UserModel(
            username="test_cli_user",
            clerk_id="clerk_cli_test",
            account_status=AccountStatusEnum.standard,
            created=datetime.utcnow(),
            updated=datetime.utcnow(),
        )
        db.add(user)
        await db.flush()

        private_key, public_key = generate_api_key(str(user.id))

        api_key = ApiKeyModel(
            user_id=user.id,
            name="cli_test_key",
            keystring=private_key[:16],
            public_key=public_key,
        )
        db.add(api_key)
        await db.commit()

    await engine.dispose()

    print(private_key)


if __name__ == "__main__":
    asyncio.run(main())
