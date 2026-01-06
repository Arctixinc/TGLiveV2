import shutil
import logging
from os import path as opath, getenv
from subprocess import run, PIPE
from dotenv import load_dotenv


def main():
    # -------------------------------------------------
    # LOGGING (UPDATER ONLY)
    # -------------------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        format="[UPDATER] %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("updater")

    # -------------------------------------------------
    # ENV
    # -------------------------------------------------
    load_dotenv("config.env", override=True)

    upstream_repo = (getenv("UPSTREAM_REPO") or "https://github.com/Arctixinc/TGLiveV2").strip()
    upstream_branch = getenv("UPSTREAM_BRANCH", "main")

    if not upstream_repo:
        logger.info("UPSTREAM_REPO not set, skipping update")
        return

    try:
        # Remove old repo safely
        if opath.exists(".git"):
            shutil.rmtree(".git")
            logger.info("Removed existing .git directory")

        update_cmd = [
            "git", "init", "-q",
        ]

        run(update_cmd, check=True)

        run(["git", "config", "user.email", "inc.arctix@gmail.com"], check=True)
        run(["git", "config", "user.name", "arctixinc"], check=True)
        run(["git", "add", "."], check=True)
        run(["git", "commit", "-sm", "update"], stdout=PIPE, stderr=PIPE)
        run(["git", "remote", "add", "origin", upstream_repo], check=True)
        run(["git", "fetch", "origin"], stdout=PIPE, stderr=PIPE)
        run(["git", "reset", "--hard", f"origin/{upstream_branch}"], check=True)

        logger.info("✅ Update successful")

    except Exception as e:
        logger.exception("❌ Updater crashed")


if __name__ == "__main__":
    main()