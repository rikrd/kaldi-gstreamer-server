# -*- coding: UTF-8 -*-
"""
Created on Jun 27, 2013

@author: tanel
"""

import unittest
from gi.repository import GObject, Gst
import thread
import logging
import subprocess
from decoder2 import DecoderPipeline2
import time
import os.path


class Ricard2Tests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(Ricard2Tests, self).__init__(*args, **kwargs)
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def setUpClass(cls):
        cls.model_dir = 'test/models/english/wsj'
        decoder_conf = {"word-syms": os.path.join(cls.model_dir, 'words.txt'),
                        "mfcc-config": os.path.join(cls.model_dir, 'conf/mfcc.conf'),
                        "ivector-extraction-config": os.path.join(cls.model_dir, 'conf/ivector_extractor.conf'),
                        "max-active": 7000,
                        "beam": 11.0,
                        "lattice-beam": 6.0,
                        "do-endpointing": True,
                        "do-phone-alignment": True,
                        "phone-syms": os.path.join(cls.model_dir, 'phones.txt'),
                        "endpoint-silence-phones": "1:2:3:4:5:6:7:8:9:10:11:12:13:14:15"}
        cls.decoder_pipeline = DecoderPipeline2({"decoder": decoder_conf})

        cls.final_hyps = []
        cls.final_aligns = []

        cls.finished = False

        cls.decoder_pipeline.set_result_handler(cls.result_getter)
        cls.decoder_pipeline.set_alignment_handler(cls.align_getter)
        cls.decoder_pipeline.set_eos_handler(cls.set_finished, cls.finished)

        loop = GObject.MainLoop()
        thread.start_new_thread(loop.run, ())

    @classmethod
    def result_getter(cls, hyp, final):
        if final:
            cls.final_hyps.append(hyp)

    @classmethod
    def align_getter(cls, hyp, final):
        if final:
            cls.final_aligns.append(hyp)

    @classmethod
    def set_finished(cls, finished):
        cls.finished = True

    def setUp(self):
        self.__class__.final_hyps = []
        self.__class__.finished = False

    def test12345678(self):
        self.decoder_pipeline.asr.set_property('model', os.path.join(self.model_dir, 'final.mdl'))
        self.decoder_pipeline.asr.set_property('fst', os.path.join(self.model_dir, 'HCLG.fst'))

        self.decoder_pipeline.init_request("test12345678",
                                           "audio/x-raw, layout=(string)interleaved, rate=(int)16000, "
                                           "format=(string)S16LE, channels=(int)1")
        adaptation_state = open("test/data/adaptation_state.txt").read()
        self.decoder_pipeline.set_adaptation_state(adaptation_state)
        f = open("test/data/english_test.raw", "rb")
        for block in iter(lambda: f.read(8000), ""):
            self.decoder_pipeline.process_data(block)

        self.decoder_pipeline.end_request()

        while not self.finished:
            time.sleep(1)
        self.assertEqual(["ONE TO A FREE FALL FIVE SIX SEVEN EIGHT", "<UNK>"], self.final_hyps)

    def testDisconnect(self):
        self.decoder_pipeline.asr.set_property('model', os.path.join(self.model_dir, 'final.mdl'))
        self.decoder_pipeline.asr.set_property('fst', os.path.join(self.model_dir, 'HCLG.fst'))

        self.decoder_pipeline.init_request("testDisconnect",
                                           "audio/x-raw, layout=(string)interleaved, rate=(int)8000, "
                                           "format=(string)S16LE, channels=(int)1")

        self.decoder_pipeline.end_request()

        while not self.finished:
            time.sleep(1)
        self.assertEqual([], self.final_hyps)

    def testForcedAlginment(self):
        self.decoder_pipeline.set_property('model', os.path.join(self.model_dir, 'final.mdl'))

        words_file = self.decoder_pipeline.get_property('word-syms')
        model_file = self.decoder_pipeline.get_property('model')
        model_dir = os.path.dirname(model_file)

        tree_file = os.path.join(model_dir, 'tree')
        lexicon_fst_file = os.path.join(model_dir, 'L_disambig.fst')
        disambig_file = os.path.join(model_dir, 'phones', 'disambig.int')

        if not os.path.isfile(tree_file) \
                or not os.path.isfile(lexicon_fst_file) \
                or not os.path.isfile(disambig_file):
            logging.error('Could not find the necessary model files: \n  {}\n  {}\n  {}'.format(tree_file,
                                                                                                lexicon_fst_file,
                                                                                                disambig_file))

        text = 'ONE TWO THREE FOUR FIVE SIX SEVEN EIGHT'
        out_fst_file = 'NEW.fst'

        cmd = '$KALDI_ROOT/src/bin/compile-train-graphs --read-disambig-syms={disambig_file} ' \
              '{tree_file} {model_file} {lm_fst_file} ' \
              '"ark:echo ''dummy_id {text}'' |' \
              ' $KALDI_ROOT/egs/wsj/s5/utils/sym2int.pl --map-oov 2 -f 2- {words_file} -|" ' \
              '"scp,p:echo dummy_id -|" > {out_fst_file}'.format(text=text,
                                                                 disambig_file=disambig_file,
                                                                 tree_file=tree_file,
                                                                 model_file=model_file,
                                                                 lm_fst_file=lexicon_fst_file,
                                                                 words_file=words_file,
                                                                 out_fst_file=out_fst_file)
        subprocess.call(cmd, shell=True)

        self.decoder_pipeline.set_property('fst', os.path.abspath(out_fst_file))

        self.decoder_pipeline.init_request("testForcedAlignment",
                                           "audio/x-raw, layout=(string)interleaved, rate=(int)16000, "
                                           "format=(string)S16LE, channels=(int)1")

        f = open("test/data/english_test.raw", "rb")
        for block in iter(lambda: f.read(8000), ""):
            self.decoder_pipeline.process_data(block)

        self.decoder_pipeline.end_request()

        while not self.finished:
            time.sleep(1)

        self.assertEqual(["ONE TWO THREE FOUR FIVE SIX SEVEN EIGHT", "ONE"], self.final_hyps)


def main():
    unittest.main()


if __name__ == '__main__':
    main()