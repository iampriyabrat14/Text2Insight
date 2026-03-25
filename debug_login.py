"""Debug login issue."""
import asyncio, sys
sys.path.insert(0, '.')

from backend.database.sqlite_manager import get_session_factory, init_db
from backend.auth.service import get_user_by_username, verify_password, hash_password

async def main():
    await init_db()
    factory = get_session_factory()
    async with factory() as db:
        for username, password in [("admin", "admin123"), ("demo", "demo123")]:
            user = await get_user_by_username(db, username)
            if not user:
                print(f"[MISSING] user '{username}' not in database")
                continue
            ok = verify_password(password, user.hashed_password)
            print(f"[{'OK' if ok else 'FAIL'}] {username}: password_match={ok}, active={user.is_active}, stored_hash={user.hashed_password[:30]}...")

        # Show what hash admin123 produces now
        print(f"\nNew hash for 'admin123': {hash_password('admin123')[:30]}...")

asyncio.run(main())
