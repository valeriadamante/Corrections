import os
import ROOT
from .CorrectionsCore import *
from Common.Utilities import WorkingPointsbTag
import yaml
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/BTagShapeCalibration
# https://twiki.cern.ch/twiki/bin/view/CMS/BTagCalibration
# https://twiki.cern.ch/twiki/bin/view/CMS/BTagSFMethods
# https://github.com/cms-btv-pog
# https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/POG/BTV
# https://twiki.cern.ch/twiki/bin/view/CMS/BtagRecommendation
# https://cms-talk.web.cern.ch/t/ul-b-tagging-sf-update/20209/2
# https://gitlab.cern.ch/cms-btv/btv-json-sf/-/tree/master/data/UL2016preVFP
# https://github.com/hh-italian-group/h-tautau/blob/master/McCorrections/src/BTagWeight.cpp
# https://btv-wiki.docs.cern.ch/PerformanceCalibration/SFUncertaintiesAndCorrelations/working-point-based-sfs-fixedwp-sfs
# https://btv-wiki.docs.cern.ch/PerformanceCalibration/fixedWPSFRecommendations/

class bTagCorrProducer:
    jsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/{}/btagging.json.gz"
    bTagEff_JsonPath = "Corrections/data/BTV/{}/btagEff.root"
    initialized = False
    uncSource_bTagWP = ["btagSFbc_uncorrelated", "btagSFlight_uncorrelated","btagSFbc_correlated", "btagSFlight_correlated"]
    uncSources_bTagShape_shape = ["FlavorQCD","RelativeBal", "HF", "BBEC1", "EC2", "Absolute", "BBEC1_", "Absolute_", "EC2_", "HF_", "RelativeSample_" ]
    uncSources_bTagShape_norm = ["lf", "hf", "lfstats1", "lfstats2", "hfstats1", "hfstats2", "cferr1", "cferr2"]

    def __init__(self, period, loadEfficiency=True):
        jsonFile = bTagCorrProducer.jsonPath.format(period)
        jsonFile_eff = os.path.join(os.environ['ANALYSIS_PATH'],bTagCorrProducer.bTagEff_JsonPath.format(period))
        if not loadEfficiency:
            jsonFile_eff = ""
        if not bTagCorrProducer.initialized:
            headers_dir = os.path.dirname(os.path.abspath(__file__))
            header_path = os.path.join(headers_dir, "btag.h")
            headershape_path = os.path.join(headers_dir, "btagShape.h")
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
            ROOT.gInterpreter.Declare(f'#include "{headershape_path}"')
            ROOT.gInterpreter.ProcessLine(f'::correction::bTagCorrProvider::Initialize("{jsonFile}", "{jsonFile_eff}")')
            ROOT.gInterpreter.ProcessLine(f"""::correction::bTagShapeCorrProvider::Initialize("{jsonFile}", "{periods[period]}")""")
            bTagCorrProducer.initialized = True

    def getWPValues(self):
        wp_values = {}
        for wp in WorkingPointsbTag:
            root_wp = getattr(ROOT.WorkingPointsbTag, wp.name)
            wp_values[wp] = ROOT.correction.bTagCorrProvider.getGlobal().getWPvalue(root_wp)
        return wp_values

    def getWPid(self, df):
        wp_values = self.getWPValues()
        df = df.Define("Jet_idbtagDeepFlavB", f"::correction::bTagCorrProvider::getGlobal().getWPBranch(Jet_btagDeepFlavB)")
        return df

    def getBTagWPSF(self, df, return_variations=True, isCentral=True):
        sf_sources = bTagCorrProducer.uncSource_bTagWP
        SF_branches = []
        sf_scales = [up, down] if return_variations else []
        for source in [ central ] + sf_sources:
            for scale in [ central ] + sf_scales:
                if source == central and scale != central: continue
                if not isCentral and scale!= central: continue
                #syst_name = source+scale if source != central else 'Central'
                syst_name = source+scale
                for wp in WorkingPointsbTag:
                    branch_name = f"weight_bTagSF_{wp.name}_{syst_name}"
                    #print(branch_name)
                    branch_central = f"""weight_bTagSF_{wp.name}_{source+central}"""
                    #branch_central = f"""weight_bTagSF_{wp.name}_{getSystName(central, central)}"""
                    df = df.Define(f"{branch_name}_double",
                                f''' ::correction::bTagCorrProvider::getGlobal().getSF(
                                Jet_p4, Jet_bCand, Jet_hadronFlavour, Jet_btagDeepFlavB, WorkingPointsbTag::{wp.name},
                                ::correction::bTagCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}) ''')
                    if scale != central:
                        branch_name_final = branch_name + '_rel'
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double/{branch_central})")
                    else:
                        if source == central:
                            branch_name_final = f"""weight_bTagSF_{wp.name}_{central}"""
                        else:
                            branch_name_final = branch_name
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double)")
                    SF_branches.append(branch_name_final)
        return df,SF_branches


    def getBTagShapeSF(self, df, isCentral, return_variations):
        sf_sources_norm = bTagCorrProducer.uncSources_bTagShape_norm
        sf_scales = [up, down] if return_variations else []
        SF_branches = []
        for source in [ central ] + sf_sources_norm:
            for scale in [ central ] + sf_scales:
                if source == central and scale != central: continue
                if not isCentral and scale!= central: continue
                syst_name = source+scale# if source != central else 'Central'
                branch_name = f"weight_bTagShape_{syst_name}"
                branch_central = f"""weight_bTagShape_{source+central}"""

                df = df.Define(f"{branch_name}_double",
                    f'''::correction::bTagShapeCorrProvider::getGlobal().getBTagShapeSF(
                    Jet_p4, Jet_bCand, Jet_hadronFlavour, Jet_btagDeepFlavB,
                    ::correction::bTagShapeCorrProvider::UncSource::{source},
                    ::correction::UncScale::{scale}
                    ) ''')

                if scale != central:
                        branch_name_final = branch_name + '_rel'
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double/{branch_central})")
                else:
                    if source == central:
                        branch_name_final = f"""weight_bTagShape_{central}"""
                    else:
                        branch_name_final = branch_name
                    df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double)")
                SF_branches.append(branch_name_final)
        return df,SF_branches


















