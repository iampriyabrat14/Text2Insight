"""Run this script to reset and re-create the two default users."""
import asyncio
import sys
sys.path.insert(0, '.')

from backend.database.sqlite_manager import init_db, get_session_factory
from backend.auth.service import create_user, hash_password, verify_password

async def main():
    await init_db()
    factory = get_session_factory()

    # Test password hashing
    h = hash_password("admin123")
    assert verify_password("admin123", h), "Hash/verify broken!"
    print("Password hashing: OK")

    async with factory() as db:
        from sqlalchemy import delete
        from backend.database.sqlite_manager import User
        # Remove existing default users
        await db.execute(delete(User).where(User.username.in_(["admin", "demo"])))
        await db.commit()

        for username, email, password, tier in [
            ("admin", "admin@example.com", "admin123", "admin"),
            ("demo",  "demo@example.com",  "demo123",  "pro"),
        ]:
            user = await create_user(db, username, email, password, tier)
            print(f"Created user: {username} (id={user.id})")

    print("\nDone. You can now login with:")
    print("  admin / admin123")
    print("  demo  / demo123")

asyncio.run(main())
