from flamesdk import FlameCoreSDK
import time


def main():
    flame = FlameCoreSDK()
    while True:
        flame.flame_log("alive")
        time.sleep(1)


if __name__ == "__main__":
    main()
