import os
dirName = os.path.dirname(__file__)
fileToRun = os.path.join(dirName, '..', 'auxTel', 'runStarTrackerNightReport.py')
print(f'Running {fileToRun}...')
os.system(f'python {fileToRun}')
