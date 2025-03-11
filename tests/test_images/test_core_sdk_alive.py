from flamesdk import FlameCoreSDK
import time


def main():
    print("Starting Flame")
    flame = FlameCoreSDK()
    while True:
        print("alive")
        time.sleep(1)


if __name__ == "__main__":
    print("main")
    main()
