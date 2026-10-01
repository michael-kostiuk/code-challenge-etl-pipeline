"""Pipeline entry point: persons ⋈ organizations -> Elasticsearch `persons` index."""
import sys

from etl.run import main

if __name__ == "__main__":
    sys.exit(main())
