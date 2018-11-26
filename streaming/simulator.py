import os, sys, time, shutil

root_dir = os.path.join(os.environ['HOME'], "Regular-test")
outdir = os.path.join(os.environ['HOME'], "Regular-stream")
#year = sys.argv[1]

def main():
    for directory, subdirectory, files in os.walk(root_dir):
        if len(subdirectory) == 0:
            dirname = os.path.join(*directory.split('/')[-5:])
            try:
                destination = os.path.join(outdir, dirname)
                shutil.copytree(directory, destination)
                print("Copied source {} to destination".format(dirname, destination))
            except OSError, e:
                print("File {} already exists".format(dirname))
            print('Found directory: %s' % dirname)
            time.sleep(3)


if __name__ == '__main__':
    main()
