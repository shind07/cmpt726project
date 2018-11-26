import os, sys, time, shutil

root_dir = os.path.join(os.environ['HOME'], "Regular-test")
#year = sys.argv[1]

def main():
    for directory, subdirectory, files in os.walk(root_dir):
        if len(subdirectory) == 0:
            dirname = os.path.join(*directory.split('/')[-5:])
            print('Found directory: %s' % dirname)
            time.sleep(5)
            for f in files:
                print('\t%s' % f)

if __name__ == '__main__':
    main()
# try:
#     text = nltk.corpus.gutenberg.raw('carroll-alice.txt')
# except LookupError:
#     nltk.download('gutenberg')
#     text = nltk.corpus.gutenberg.raw('carroll-alice.txt')
#
# lines = [l.strip() for l in text.splitlines() if len(l.strip()) > 0]
#
# os.makedirs(outdir, exist_ok=True)
# print("Starting file stream...")
# while True:
#     subset = random.sample(lines, random.randint(5, 10))
#     uu = str(uuid.uuid4())
#     filename = os.path.join(outdir, uu+'.txt')
#     with open(filename, 'w') as outfh:
#         outfh.write('\n'.join(subset))
#     time.sleep(random.random()*3)
