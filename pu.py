import os
import ROOT
from .CorrectionsCore import *
# lumi POG: https://twiki.cern.ch/twiki/bin/view/CMS/TWikiLUM
# PU scenarios https://twiki.cern.ch/twiki/bin/view/CMS/PileupScenariosRun2
# https://bamboo-hep.readthedocs.io/en/latest/recipes.html#pileup-reweighting
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData

class puWeightProducer:
    jsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/LUM/{}/puWeights.json.gz"
    initialized = False

    uncSource = ['pu']
    golden_json_dict = {
        "2018_UL":"Collisions18_UltraLegacy_goldenJSON",
        "2017_UL": "Collisions17_UltraLegacy_goldenJSON",
        "2016preVFP_UL":"Collisions16_UltraLegacy_goldenJSON",
        "2016postVFP_UL":"Collisions16_UltraLegacy_goldenJSON",
    }
    def __init__(self, period):
        jsonFile = puWeightProducer.jsonPath.format(period)
        if not puWeightProducer.initialized:
            headers_dir = os.path.dirname(os.path.abspath(__file__))
            header_path = os.path.join(headers_dir, "pu.h")
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
            ROOT.gInterpreter.ProcessLine(f'::correction::puCorrProvider::Initialize("{jsonFile}", "{self.golden_json_dict[period]}")')
            puWeightProducer.initialized = True

    def getWeight(self, df,return_variations=True, isCentral=True):
        sf_sources =puWeightProducer.uncSource if return_variations else []
        weights = {}
        for source in [ central ] + sf_sources:
            for scale in getScales(source):
                if not isCentral and scale!= central: continue
                syst_name = getSystName(source, scale)
                weights[syst_name] = []
                df = df.Define(f'puWeight_{scale}', f'''::correction::puCorrProvider::getGlobal().getWeight(
                                ::correction::UncScale::{scale}, Pileup_nTrueInt)''')
                weights[syst_name].append(f'puWeight_{scale}')
        return df,weights


