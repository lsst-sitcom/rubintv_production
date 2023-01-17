import os
dirName = os.path.dirname(__file__)
fileToRun = os.path.join(dirName, '..', 'auxTel', 'runNightReporter.py')
print(f'Running {fileToRun}...')
os.system(f'python {fileToRun}')
