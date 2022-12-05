import logging
from time import sleep
from lsst.daf.butler import Butler

while True:
    try:
        logger = logging.getLogger('lsst.rubintv.production')
        logger.warning('Imports worked...')
        print('Imports worked...')

        collections = ['LSSTCam/raw/all', 'LSSTCam/calib']
        butler = Butler('/repo/ir2/butler.yaml', collections=collections, instrument='LSSTCam')

        logger.warning('Butler init worked!')
        print('Butler init worked!')

        filename = '/scratch/test.txt'
        msg = 'This is the test message'
        with open(filename, 'w') as f:
            f.write(msg)

        logger.warning('Wrote file to scratch.')
        print('Wrote file to scratch.')

        with open(filename, 'r') as f:
            read = f.read()

        logger.warning('Read file back from scratch.')
        print('Read file back from scratch.')

        assert read==msg

        logger.warning(f'Data was the same.')
        print(f'Data was the same.')
        sleep(100)

    except Exception as e:
        logger.warning(f'Caught exception {e}')
        sleep(100)
        continue
