import sys

from airbyte_cdk.entrypoint import launch
from source_superleap_crm import SourceSuperleapCrm


def main():
    source = SourceSuperleapCrm()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    main()
