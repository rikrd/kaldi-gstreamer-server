from __future__ import with_statement
import glob
from fabric.api import env, cd, get, lcd
import os


env.use_ssh_config = True
env.hosts = ['iceberg']
env.shell = 'qrsh'

kaldi_path = '/data/ac1rpm/kaldi'


def _find_replace(filename, before, after):
    with open(filename, 'r') as f:
        text_before = f.read()

    text_after = text_before.replace(before, after)

    with open(filename, 'w') as f:
        f.write(text_after)


def get_wsj_model(recipe_path=os.path.join(kaldi_path, 'egs/wsj/s5'),
                  model_path='exp/nnet2_online/nnet_ms_a_online',
                  graph_path='exp/tri4b/graph_tgpr',
                  lang_path='data/lang',
                  output_dir='test/models/english/wsj'):
    """Download a Kaldi model from a Kaldi recipe.

    :param recipe_path: Recipe absolute path (e.g. /path/to/kaldi/egs/wsj/s5)
    :param model_path: Model path relative to recipe path (e.g. exp/nnet2_online/nnet_ms_a_online)
    :param graph_path: Graph path relative to recipe path (e.g. exp/tri4b/graph_tgpr)
    :param lang_path: Language path relative to recipe path (e.g. data/lang)
    :param output_dir: Output directory
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

    lang_files = ['L.fst',
                  'L_disambig.fst']

    # Create the output directory if necessary
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # Get the files from the recepie output
    with lcd(output_dir):
        with cd(recipe_path):

            for filename in model_files:
                get(os.path.join(model_path, filename),
                    local_path='%(path)s')

            for filename in graph_files:
                get(os.path.join(graph_path, filename),
                    local_path='%(path)s')

            for filename in lang_files:
                get(os.path.join(lang_path, filename),
                    local_path='%(path)s')

    # Replace the paths make them relative to ..
    for filename in glob.glob(os.path.join(output_dir, 'conf', '*.conf')):
        _find_replace(filename,
                      os.path.join(recipe_path, model_path),
                      os.path.relpath(output_dir, start=os.path.abspath('kaldigstserver')))
