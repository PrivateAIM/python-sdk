from flamesdk import FlameCoreSDK
import time


def main():
    flame = FlameCoreSDK()
    flame.get_fhir_data("fhir/Observation?_count=500")


if __name__ == "__main__":
    main()