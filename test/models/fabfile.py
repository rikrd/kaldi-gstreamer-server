from __future__ import with_statement
import glob
from fabric.api import env, local, settings, run, cd, prefix, put, get, lcd
from fabric.contrib.files import sed
import os


env.use_ssh_config = True
env.hosts = ['iceberg']
env.shell = 'qrsh'

work_path = 'dev/cloudcast'
kaldi_path = '/data/ac1rpm/kaldi'


def _find_replace(filename, before, after):
    with open(filename, 'r') as f:
        text_before = f.read()

    text_after = text_before.replace(before, after)

    with open(filename, 'w') as f:
        f.write(text_after)


def get_wsj_model(model='exp/nnet2_online/nnet_ms_a_online', graph='exp/tri4b/graph_tgpr', lang='data/lang',
                  output_dir='english/wsj'):
    """Download the WSJ model.

    :return:
    """
    model_files = ['final.mdl',
                   'conf',
                   'ivector_extractor',
                   'tree']

    graph_files = ['HCLG.fst',
                   'words.txt',
                   'phones.txt',
                   'phones']

    lang_files = ['L.fst']

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # Get the files from the recepie output
    with lcd(output_dir):
        with cd(os.path.join(kaldi_path, 'egs/wsj/s5')):

            for filename in model_files:
                get(os.path.join(model, filename),
                    local_path='%(path)s')

            for filename in graph_files:
                get(os.path.join(graph, filename),
                    local_path='%(path)s')

            for filename in lang_files:
                get(os.path.join(lang, filename),
                    local_path='%(path)s')

    # Replace the paths make them relative to ..
    for filename in glob.glob(os.path.join(output_dir, 'conf', '*.conf')):
        _find_replace(filename,
                      os.path.join(kaldi_path, 'egs/wsj/s5', model),
                      os.path.relpath(output_dir, start=os.path.abspath('../..')))
