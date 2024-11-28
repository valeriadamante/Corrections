import os
import ROOT
from .CorrectionsCore import *
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammaUL2016To2018
# https://github.com/cms-egamma/ScaleFactorsJSON?tab=readme-ov-file
# https://twiki.cern.ch/twiki/bin/view/CMS/EgammaSFJSON
# https://twiki.cern.ch/twiki/bin/view/CMS/EgammaPOG
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammaUL2016To2018#Access_of_SFs_using_JSON_files
# https://twiki.cern.ch/twiki/bin/view/CMS/EgHLTScaleFactorMeasurements
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammaUL2016To2018#Access_of_SFs_using_JSON_files
# https://twiki.cern.ch/twiki/bin/view/CMS/EGMHLTRun3RecommendationForPAG
# https://twiki.cern.ch/twiki/bin/view/CMS/ElectronScaleFactorsRun2#EGM_certified_vs_custom_self_pro

class EleCorrProducer:
    EleID_JsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/{}/electron.json.gz"
    EleES_JsonPath = "Corrections/data/EGM/{}/EGM_ScaleUnc.json.gz"
    initialized = False
    ID_sources = ["EleID"]
    working_points = ["wp80iso", "wp80noiso"]
    year = ""

    def __init__(self, period):
        EleID_JsonFile = EleCorrProducer.EleID_JsonPath.format(period)
        EleES_JsonFile = os.path.join(os.environ['ANALYSIS_PATH'],EleCorrProducer.EleES_JsonPath.format(period))

        if not EleCorrProducer.initialized:
            headers_dir = os.path.dirname(os.path.abspath(__file__))
            header_path = os.path.join(headers_dir, "electron.h")
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
            ROOT.gInterpreter.ProcessLine(f'::correction::EleCorrProvider::Initialize("{EleID_JsonFile}", "{EleES_JsonFile}")')
            EleCorrProducer.year = period.split("_")[0]
            EleCorrProducer.initialized = True



    def getIDSF(self, df, lepton_legs, isCentral, return_variations):
        sf_sources =EleCorrProducer.ID_sources
        SF_branches = []
        sf_scales = [up, down] if return_variations else []
        for working_point in EleCorrProducer.working_points:
            for source in sf_sources:
                for scale in [central]+sf_scales:
                    if not isCentral and scale!= central: continue
                    #syst_name = getSystName(source, scale)
                    for leg_idx, leg_name in enumerate(lepton_legs):
                        branch_name = f"weight_{leg_name}_EleSF_{working_point}_{source+scale}"
                        branch_central = f"""weight_{leg_name}_EleSF_{working_point}_{source+central}"""
                        #print(branch_name)
                        #print(branch_central)
                        df = df.Define(f"{branch_name}_double",
                                    f'''HLepCandidate.leg_type[{leg_idx}] == Leg::e ? ::correction::EleCorrProvider::getGlobal().getID_SF(
                                HLepCandidate.leg_p4[{leg_idx}], Electron_genMatch.at(HLepCandidate.leg_index[{leg_idx}]), "{working_point}",
                                "{EleCorrProducer.year}",::correction::EleCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}) : 1.;''')
                        if scale != central:
                            branch_name_final = branch_name + '_rel'
                            df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double/{branch_central})")
                        else:
                            if source == central:
                                branch_name_final = f"""weight_{leg_name}_EleSF_{central}"""
                            else:
                                branch_name_final = branch_name
                            df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double)")
                        SF_branches.append(branch_name_final)
        return df,SF_branches
