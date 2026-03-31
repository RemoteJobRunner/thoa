"""
Helper script: run with backend's venv python from the backend directory.
Creates a file record, dataset, and a completed job with output_context
so that dataset download can be tested against Azurite.

Prints to stdout as JSON:
  {"user_public_id": "...", "file_public_id": "...", "dataset_public_id": "..."}
"""
import asyncio
import json
import sys
import os


async def main():
    from tests.fixtures.test_settings import configure_test_settings
    configure_test_settings()

    from db.session import create_engine, create_sessionmaker
    from db.models.models import UserModel, FileModel, DatasetModel, JobModel, JobStatusEnum
    from db.models.associations import dataset_files_association
    from sqlalchemy import select, insert
    from datetime import datetime

    engine = create_engine()
    sessionmaker = create_sessionmaker(engine)

    async with sessionmaker() as db:
        # Load the test user created by setup_test_db.py
        result = await db.execute(
            select(UserModel).where(UserModel.username == "test_cli_user")
        )
        user = result.scalars().first()
        if not user:
            print("ERROR: test_cli_user not found — run setup_test_db.py first", file=sys.stderr)
            sys.exit(1)

        # Create file record
        file = FileModel(
            user_id=user.id,
            filename="hello.txt",
            size=15,
            hash="6cd3556deb0da54bca060b4c39479839",
        )
        db.add(file)
        await db.flush()

        # Create dataset linked to the file
        dataset = DatasetModel(
            user_id=user.id,
            remaining_downloads=10,
        )
        db.add(dataset)
        await db.flush()
        await db.execute(
            insert(dataset_files_association).values(dataset_id=dataset.id, file_id=file.id)
        )
        await db.flush()

        # output_context maps staging path → file public_id
        # adjust_context() will strip the common prefix leaving just "hello.txt"
        staging_path = f"/thoa_job_data/staging/{user.public_id}/hello.txt"
        output_context = {staging_path: str(file.public_id)}

        job = JobModel(
            user_id=user.id,
            output_dataset_id=dataset.id,
            output_context=output_context,
            status=JobStatusEnum.completed,
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            requested_ram=4,
            requested_cpu=2,
            requested_disk_space=10,
        )
        db.add(job)
        await db.commit()

    await engine.dispose()

    print(json.dumps({
        "user_public_id": str(user.public_id),
        "file_public_id": str(file.public_id),
        "dataset_public_id": str(dataset.public_id),
    }))


if __name__ == "__main__":
    asyncio.run(main())
