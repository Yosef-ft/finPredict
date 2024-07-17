import logging



logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')

file_handler = logging.FileHandler('info.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


if __name__ == "__main__":

    while True:    
        input_str = input("Do you need more data (y/n): ").lower()

        if input_str == 'n':
            print('Exiting main window')
            break

        elif input_str == 'y':
            symbol = input('Enter the symbol: ').upper()



