import os
import ROOT
from .CorrectionsCore import *
from Common.Utilities import *
import yaml
# Tau JSON POG integration for tau legs in eTau, muTau, diTau:
# # https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/POG/TAU?ref_type=heads

# singleEle + e/mu legs for xTriggers eTau, muTau:
# https://twiki.cern.ch/twiki/bin/view/CMS/EgHLTScaleFactorMeasurements

# singleMu : files taken from https://gitlab.cern.ch/cms-muonPOG/muonefficiencies/-/tree/master/Run2/UL and saved locally

# singleTau: https://twiki.cern.ch/twiki/bin/viewauth/CMS/TauTrigger#Run_II_Trigger_Scale_Factors
# singleTau: Legacy bc there are no UL as mentioned herehttps://cms-pub-talk.web.cern.ch/t/tau-pog-review/8404/4
# singleTau: 2016 - (HLT_VLooseIsoPFTau120_Trk50_eta2p1_v OR HLT_VLooseIsoPFTau140_Trk50_eta2p1_v) - 0.88 +/- 0.08
# singleTau: 2017 - HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_v - 1.08 +/- 0.10
# singleTau: 2018 - (HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_v) - 	0.87 +/- 0.11

year_singleElefile = {
    "2018_UL":"sf_el_2018_HLTEle32.root",
    "2017_UL":"sf_el_2017_HLTEle32.root",
    "2016preVFP_UL":"sf_el_2016pre_HLTEle25.root",
    "2016postVFP_UL":"sf_el_2016post_HLTEle25.root"
}

year_singleMufile = {
    "2018_UL":"Efficiencies_muon_generalTracks_Z_Run2018_UL_SingleMuonTriggers.root",
    "2017_UL":"Efficiencies_muon_generalTracks_Z_Run2017_UL_SingleMuonTriggers.root",
    "2016preVFP_UL":"Efficiencies_muon_generalTracks_Z_Run2016_UL_HIPM_SingleMuonTriggers.root",
    "2016postVFP_UL":"Efficiencies_muon_generalTracks_Z_Run2016_UL_SingleMuonTriggers.root"
}

year_xTrg_eTaufile = {
    "2018_UL":"sf_el_2018_HLTEle24Tau30.root",
    "2017_UL":"sf_el_2017_HLTEle24Tau30.root",
    "2016preVFP_UL":"sf_mu_2016pre_HLTMu20Tau27.root",
    "2016postVFP_UL":"sf_mu_2016post_HLTMu20Tau27.root"
}

year_xTrg_muTaufile = {
    "2018_UL":"sf_mu_2018_HLTMu20Tau27.root",
    "2017_UL":"sf_mu_2017_HLTMu20Tau27.root",
    "2016preVFP_UL":"sf_mu_2016pre_HLTMu20Tau27.root",
    "2016postVFP_UL":"sf_mu_2016post_HLTMu20Tau27.root"
}

year_METfile = {
    "2018_UL":"150_mumu_fit_2018.root",
    "2017_UL":"150_mumu_fit_2017.root",
    "2016preVFP_UL":"150_mumu_fit_2016APV.root",
    "2016postVFP_UL":"150_mumu_fit_2016.root"
}

class TrigCorrProducer:
    TauTRG_jsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/TAU/{}/tau.json.gz"
    MuTRG_jsonPath = "Corrections/data/TRG/{0}/{1}"
    eTRG_jsonPath = "Corrections/data/TRG/{0}/{1}"
    mu_XTrg_jsonPath = "Corrections/data/TRG/{0}/{1}"
    e_XTrg_jsonPath = "Corrections/data/TRG/{0}/{1}"
    MET_jsonPath =  "Corrections/data/TRG/{0}/{1}"
    initialized = False
    deepTauVersion = 'DeepTau2017v2p1'
    SFSources = { 'ditau': [ "ditau_DM0","ditau_DM1", "ditau_3Prong"], 'singleMu':['singleMu'], 'singleMu50':['singleMu50or24','singleMu50'],'singleTau':['singleTau'], 'singleEle':['singleEle'],'etau':['etau_ele',"etau_DM0","etau_DM1", "etau_3Prong",],'mutau':['mutau_mu',"mutau_DM0","mutau_DM1", "mutau_3Prong"], 'MET':['MET']}

    muon_trg_dict = {
        "2018_UL": ROOT.std.vector('std::string')({"NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight","NUM_IsoMu24_or_Mu50_DEN_CutBasedIdTight_and_PFIsoTight", "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"}),

        "2017_UL": ROOT.std.vector('std::string')({"NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight","NUM_IsoMu27_or_Mu50_DEN_CutBasedIdTight_and_PFIsoTight", "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"}),

        "2016preVFP_UL":ROOT.std.vector('std::string')({"NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight","NUM_IsoMu24_or_IsoTkMu24_or_Mu50_or_TkMu50_DEN_CutBasedIdTight_and_PFIsoTight", "NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"}),

        "2016postVFP_UL":ROOT.std.vector('std::string')({"NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight","NUM_IsoMu24_or_IsoTkMu24_or_Mu50_or_TkMu50_DEN_CutBasedIdTight_and_PFIsoTight", "NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"})

    }

    muon_trgHistNames_eff_dict = {
        "2018_UL": ["NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight_abseta_pt"],

        "2017_UL": ["NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight_abseta_pt"],

        "2016preVFP_UL":["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight_abseta_pt"],

        "2016postVFP_UL":["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight_abseta_pt"]

    }

    muon_trgHistNames_dict = {
        "2018_UL": ["NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst","NUM_IsoMu24_or_Mu50_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst", "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose_eta_pt_syst"],

        "2017_UL": ["NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst","NUM_IsoMu27_or_Mu50_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst", "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose_eta_pt_syst"],

        "2016preVFP_UL":["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst","NUM_IsoMu24_or_IsoTkMu24_or_Mu50_or_TkMu50_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst", "NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose_eta_pt_syst"],

        "2016postVFP_UL":["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst","NUM_IsoMu24_or_IsoTkMu24_or_Mu50_or_TkMu50_DEN_CutBasedIdTight_and_PFIsoTight_eta_pt_syst", "NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose_eta_pt_syst"],

    }

    singleTau_SF_dict = {
        "2018_UL": {'Central': 0.87 , 'Up': 0.98 , 'Down': 0.76,},
        "2017_UL" :  {'Central': 1.08, 'Up': 1.18, 'Down': 0.98,},
        "2016preVFP_UL" :  {'Central':0.88, 'Up':0.8, 'Down':0.96,},
        "2016postVFP_UL" :   {'Central':0.88, 'Up':0.8, 'Down':0.96,},
    }

    def __init__(self, period, config):
        jsonFile_Tau = TrigCorrProducer.TauTRG_jsonPath.format(period)
        self.deepTauVersion = f"""DeepTau{deepTauVersions[config["deepTauVersion"]]}v{config["deepTauVersion"]}"""
        jsonFile_e = os.path.join(os.environ['ANALYSIS_PATH'],TrigCorrProducer.eTRG_jsonPath.format(period, year_singleElefile[period]))
        jsonFile_Mu = os.path.join(os.environ['ANALYSIS_PATH'],TrigCorrProducer.MuTRG_jsonPath.format(period, year_singleMufile[period]))
        jsonFile_mu_XTrg = os.path.join(os.environ['ANALYSIS_PATH'],TrigCorrProducer.mu_XTrg_jsonPath.format(period,year_xTrg_muTaufile[period]))
        jsonFile_e_XTrg = os.path.join(os.environ['ANALYSIS_PATH'],TrigCorrProducer.e_XTrg_jsonPath.format(period,year_xTrg_eTaufile[period]))

        jsonFile_MET = os.path.join(os.environ['ANALYSIS_PATH'],TrigCorrProducer.MET_jsonPath.format(period,year_METfile[period]))
        self.period = period
        self.year = period.split('_')[0]
        #self.trg_config = trg_config
        #print(jsonFile_e_XTrg)
        if self.deepTauVersion=='DeepTau2018v2p5':
            jsonFile_Tau_rel = f"Corrections/data/TAU/{period}/tau_DeepTau2018v2p5_{period}_101123.json"
            jsonFile_Tau = os.path.join(os.environ['ANALYSIS_PATH'],jsonFile_Tau_rel)
        if not TrigCorrProducer.initialized:
            headers_dir = os.path.dirname(os.path.abspath(__file__))
            header_path = os.path.join(headers_dir, "triggers.h")
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
            wp_map_cpp = createWPChannelMap(config["deepTauWPs"])
            #print(wp_map_cpp)
            # "{self.muon_trg_dict[period]}",
            year = period.split('_')[0]
            trigNames_mu_vec = """std::vector<std::string>{\""""
            trigNames_mu_vec += """\", \"""".join(path for path in self.muon_trgHistNames_dict[period])
            trigNames_mu_vec += """\" } """
            effNames_mu_vec = """std::vector<std::string>{\""""
            effNames_mu_vec += """\", \"""".join(path for path in self.muon_trgHistNames_eff_dict[period])
            effNames_mu_vec += """\" } """
            #print(trigNames_mu_vec)
            ROOT.gInterpreter.ProcessLine(f"""::correction::TrigCorrProvider::Initialize("{jsonFile_Tau}", "{self.deepTauVersion}", {wp_map_cpp}, "{jsonFile_Mu}", "{year}", {trigNames_mu_vec},{effNames_mu_vec},"{jsonFile_e}","{jsonFile_e_XTrg}","{jsonFile_mu_XTrg}", "{jsonFile_MET}")""")
            TrigCorrProducer.initialized = True

    def addSFsbranches(self,df,sf_sources,sf_scales,isCentral,leg_idx,leg_name,trg_name,applyTrgBranch_name, SF_branches,fromCorrLib=False):
        # print(f"processing {trg_name} for {leg_idx} {leg_name}")
        for source in sf_sources:
            # print(f"processing {source}")
            for scale in sf_scales:
                # print(f"processing {scale}")
                if not isCentral and scale!= central: continue
                branch_SF_name_prefix = f"weight_{leg_name}_TrgSF"
                branch_SF_central = f"{branch_SF_name_prefix}_{source}Central"
                branch_eff_data_prefix = f"eff_data_{leg_name}_Trg"
                branch_eff_data_central = f"{branch_eff_data_prefix}_{source}Central"
                branch_eff_MC_prefix = f"eff_MC_{leg_name}_Trg"
                branch_eff_MC_central = f"{branch_eff_MC_prefix}_{source}Central"
                branch_SF_name = f"{branch_SF_name_prefix}_{source}{scale}"
                branch_eff_data_name = branch_eff_data_central if scale == central else  f"{branch_eff_data_prefix}_{source}{scale}"
                branch_eff_MC_name = branch_eff_MC_central if scale == central else  f"{branch_eff_MC_prefix}_{source}{scale}"
                # print(f"branch name will be {branch_SF_name}")

                if f"{branch_SF_name}_double" in df.GetColumnNames():
                    # print(f"{branch_SF_name}_double in df cols" )
                    continue
                if fromCorrLib == True:
                    df = df.Define(f"{branch_SF_name}_double",
                            f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getTauSF_fromCorrLib(
                            HLepCandidate.leg_p4[{leg_idx}], Tau_decayMode.at(HLepCandidate.leg_index[{leg_idx}]), "{trg_name}", HLepCandidate.channel(), ::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale} ) : 1.f;''')
                    df = df.Define(f"{branch_eff_data_name}",
                            f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getTauEffData_fromCorrLib(
                            HLepCandidate.leg_p4[{leg_idx}], Tau_decayMode.at(HLepCandidate.leg_index[{leg_idx}]), "{trg_name}", HLepCandidate.channel(), ::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale} ) : 1.f;''')
                    df = df.Define(f"{branch_eff_MC_name}",
                                f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getTauEffMC_fromCorrLib(
                                    HLepCandidate.leg_p4[{leg_idx}], Tau_decayMode.at(HLepCandidate.leg_index[{leg_idx}]), "{trg_name}", HLepCandidate.channel(), ::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale} ): 1.f;''')
                else:
                    df = df.Define(f"{branch_SF_name}_double",
                                f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getSF_fromRootFile(
                                HLepCandidate.leg_p4[{leg_idx}],::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale} ) : 1.f''')
                    df = df.Define(f"{branch_eff_data_name}",
                                f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getEffData_fromRootFile(
                                HLepCandidate.leg_p4[{leg_idx}],::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, true) : 1.f''')
                    df = df.Define(f"{branch_eff_MC_name}",
                                f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getEffMC_fromRootFile(
                                HLepCandidate.leg_p4[{leg_idx}],::correction::TrigCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, true) : 1.f''')
                if scale != central:
                    df = df.Define(f"{branch_SF_name}_rel", f"static_cast<float>({branch_SF_name}_double/{branch_SF_central})")
                    branch_SF_name += '_rel'
                else:
                    df = df.Define(f"{branch_SF_name}", f"static_cast<float>({branch_SF_name}_double)")
                SF_branches.append(f"{branch_SF_name}")
                SF_branches.append(f"{branch_eff_data_name}")
                SF_branches.append(f"{branch_eff_MC_name}")
        # print(f"now sf branches are {SF_branches}")
        return df,SF_branches

    def addMETBranch(self, df,sf_sources,sf_scales,isCentral,trg_name,applyTrgBranch_name, SF_branches):
        for source in sf_sources:
            for scale in sf_scales:
                if not isCentral and scale!= central: continue
                branch_SF_central = f"weight_TrgSF_{source}Central"
                branch_SF_name = f"weight_TrgSF_{source}{scale}"
                df = df.Define(f"{branch_SF_name}_double",
                                f'''{applyTrgBranch_name} ? ::correction::TrigCorrProvider::getGlobal().getMETTrgSF(
                                "{self.year}",metnomu_pt, metnomu_phi, ::correction::UncScale::{scale} ) : 1.f''')
                if scale != central:
                    df = df.Define(f"{branch_SF_name}_rel", f"static_cast<float>({branch_SF_name}_double/{branch_SF_central})")
                    branch_SF_name += '_rel'
                else:
                    df = df.Define(f"{branch_SF_name}", f"static_cast<float>({branch_SF_name}_double)")
                SF_branches.append(f"{branch_SF_name}")
        return df,SF_branches

    def addSingleTauBranch(self, df,sf_sources,sf_scales,isCentral,trg_name,leg_idx,applyTrgBranch_name, SF_branches):
        for source in sf_sources:
            for scale in sf_scales:
                if not isCentral and scale!= central: continue
                branch_SF_name_prefix = f"weight_tau{leg_idx+1}_TrgSF"
                # branch_SF_name = f"{branch_SF_name_prefix}_{source}{scale}"
                branch_SF_central = f"{branch_SF_name_prefix}_{source}Central"
                branch_SF_name = f"{branch_SF_name_prefix}_{source}{scale}"
                value_shifted = self.singleTau_SF_dict[self.period][scale]

                df = df.Define(f"{branch_SF_name}_double",
                                f'''{applyTrgBranch_name} ? {value_shifted} : 1.f''')
                if scale != central:
                    df = df.Define(f"{branch_SF_name}_rel", f"static_cast<float>({branch_SF_name}_double/{branch_SF_central})")
                    branch_SF_name += '_rel'
                else:
                    df = df.Define(f"{branch_SF_name}", f"static_cast<float>({branch_SF_name}_double)")
                SF_branches.append(f"{branch_SF_name}")
        return df,SF_branches

    def getSF(self, df, trigger_names, lepton_legs, return_variations, isCentral):
        SF_branches = []
        legs_to_be ={
            'mutau' : ['mu','tau'],
            'etau':['e','tau'],
            'ditau':['tau','tau'],
            'singleEle':['e','e'],
            'singleMu' : ['mu','mu']
        }
        for trg_name in ['mutau','etau','ditau','singleMu', 'singleEle']:
            if trg_name not in trigger_names: continue
            sf_sources = TrigCorrProducer.SFSources[trg_name]
            sf_scales = [central, up, down ] if return_variations else [ central ]
            for leg_idx, leg_name in enumerate(lepton_legs):
                # print(leg_name)
                applyTrgBranch_name = f"{trg_name}_{leg_name}_ApplyTrgSF"
                # applyTrgBranch_name_condition = "true"
                leg_to_be = legs_to_be[trg_name][leg_idx]
                apply_corrlib = leg_to_be == 'tau'
                # print(leg_to_be)
                applyTrgBranch_name_condition = f"""HLepCandidate.leg_type[{leg_idx}] == Leg::{leg_to_be} && HLT_{trg_name} && {leg_name}_HasMatching_{trg_name}"""
                # print(applyTrgBranch_name_condition)
                df = df.Define(applyTrgBranch_name, applyTrgBranch_name_condition)
                df,SF_branches= self.addSFsbranches(df,sf_sources,sf_scales,isCentral,leg_idx,leg_name,trg_name,applyTrgBranch_name, SF_branches,apply_corrlib)

        MET_trg = "MET"
        if MET_trg in trigger_names:
            sf_sources = TrigCorrProducer.SFSources[MET_trg]
            sf_scales = [central, up, down ] if return_variations else [ central ]
            applyTrgBranch_name = f"{MET_trg}_ApplyTrgSF"
            applyTrgBranch_name_condition = f"""HLT_{MET_trg}"""
            df = df.Define(applyTrgBranch_name,applyTrgBranch_name_condition)
            df,SF_branches= self.addMETBranch(df,sf_sources,sf_scales,isCentral,MET_trg,applyTrgBranch_name, SF_branches)

        singleTau_trg = 'singleTau'
        if singleTau_trg in trigger_names:

            sf_sources = TrigCorrProducer.SFSources[singleTau_trg]
            sf_scales = [central, up, down ] if return_variations else [ central ]
            for leg_idx, leg_name in enumerate(lepton_legs):
                applyTrgBranch_name = f"{singleTau_trg}_{leg_name}_ApplyTrgSF"
                applyTrgBranch_name_condition = f"""HLepCandidate.leg_type[{leg_idx}] == Leg::{leg_to_be} && HLT_{trg_name} && {leg_name}_HasMatching_{trg_name}"""
                df = df.Define(applyTrgBranch_name, applyTrgBranch_name_condition)
                df,SF_branches= self.addSingleTauBranch(df,sf_sources,sf_scales,isCentral,singleTau_trg,leg_idx,applyTrgBranch_name, SF_branches)
        return df,SF_branches