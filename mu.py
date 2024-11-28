import os
import ROOT
from .CorrectionsCore import *

# https://twiki.cern.ch/twiki/bin/viewauth/CMS/SWGuideMuonSelection
# https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/POG/MUO
# note: at the beginning of february 2024, the names have been changed to muon_Z.json.gz and muon_HighPt.json.gz for high pT muons
# https://twiki.cern.ch/twiki/bin/view/CMS/MuonUL2018
# https://twiki.cern.ch/twiki/bin/view/CMS/MuonUL2017
# https://twiki.cern.ch/twiki/bin/view/CMS/MuonUL2016



class MuCorrProducer:
    muIDEff_JsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/{}/muon_Z.json.gz"
    HighPtmuIDEff_JsonPath = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/{}/muon_HighPt.json.gz"
    initialized = False

    ##### dictionaries containing ALL uncertainties ######

    #### high pt ID (NO MORE NEEDED) ####
    highPtMu_SF_Sources_dict = {
        # reco SF
        "NUM_GlobalMuons_DEN_TrackerMuonProbes":"Reco", # --> This is the recommended one for RECO!!
        # ID SF - with tracker muons - RECOMMENDED
        "NUM_TightID_DEN_GlobalMuonProbes": "TightID",
        "NUM_HighPtID_DEN_GlobalMuonProbes": "HighPtID",
        # Iso SF
        "NUM_probe_TightRelTkIso_DEN_HighPtProbes": "HighPtIdRelTkIso",
    }

    ##### ID + trigger ####
    muID_SF_Sources_dict = {
        # reco SF
        "NUM_TrackerMuons_DEN_genTracks":"Reco", # --> This is the recommended one for RECO!!
        # ID SF - with genTracks - NOT RECOMMENDED --> WE DO NOT USE THIS
        "NUM_MediumPromptID_DEN_genTracks":"MediumID", # medium ID - NOT NEEDED NOW BECAUSE WE DON'T USE MEDIUM ID
        "NUM_TightID_DEN_genTracks":"TightID", # tight ID
        "NUM_HighPtID_DEN_genTracks": "HighPtID",# HighPtID

        # ID SF - with tracker muons - RECOMMENDED
        "NUM_MediumPromptID_DEN_TrackerMuons":"MediumID_Trk", # medium ID - NOT NEEDED NOW BECAUSE WE DON'T USE MEDIUM ID
        "NUM_TightID_DEN_TrackerMuons":"TightID_Trk", # tight ID
        "NUM_HighPtID_DEN_TrackerMuons": "HighPtID_Trk",# HighPtID ID

        # Iso SF
        "NUM_TightRelIso_DEN_MediumPromptID":"MediumRelIso", # medium ID, tight iso # NOT NEEDED NOW BECAUSE WE DON'T USE MEDIUM ID
        "NUM_TightRelIso_DEN_TightIDandIPCut":"TightRelIso", # tight ID, tight iso
        "NUM_TightRelTkIso_DEN_TrkHighPtIDandIPCut" :"HighPtIdRelTkIso",  # highPtID, tight tkRelIso

        # Trigger
        "NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight":"TightIso24", # trg --> FOR ALL PT RANGE!!
        "NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight":"TightIso27", # trg --> FOR ALL PT RANGE!!
        "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose":"Mu50", # trg --> FOR ALL PT RANGE!!
        "NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose":"Mu50_tkMu50", # trg --> FOR ALL PT RANGE!!
        "NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight": "TightIso24OrTightIsoTk24", # trg --> FOR ALL PT RANGE!!
    }

    #muID_SF_Sources = []
    ####### in these lists there are the uncertainties we will consider #######

    # for muon ID --> we consider only sources related to TightID
    muReco_SF_sources = ["NUM_TrackerMuons_DEN_genTracks"]
    muID_SF_Sources = ["NUM_TightID_DEN_TrackerMuons"]
    muIso_SF_Sources = ["NUM_TightRelIso_DEN_TightIDandIPCut"]

    # for high pt id
    highPtmuReco_SF_sources = ["NUM_GlobalMuons_DEN_TrackerMuonProbes"]
    highPtmuID_SF_Sources = ["NUM_TightID_DEN_GlobalMuonProbes", "NUM_HighPtID_DEN_GlobalMuonProbes"]
    highPtmuIso_SF_Sources = ["NUM_probe_TightRelTkIso_DEN_HighPtProbes"] # not find the tightID with tight PF iso

    # trigger
    year_unc_dict= {
        "2018_UL": ["NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight"], #,"NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"], # for HLT_mu50
        "2017_UL": ["NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight"], #, "NUM_Mu50_or_OldMu100_or_TkMu100_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"], # for HLT_mu50
        "2016preVFP_UL": ["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight" ], #,"NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"], # for HLT_mu50
        "2016postVFP_UL":["NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight" ], #,"NUM_Mu50_or_TkMu50_DEN_CutBasedIdGlobalHighPt_and_TkIsoLoose"], # for HLT_mu50
    }
    period = None


    def __init__(self, period):
        jsonFile_eff = os.path.join(os.environ['ANALYSIS_PATH'],MuCorrProducer.muIDEff_JsonPath.format(period))
        jsonFile_eff_highPt = os.path.join(os.environ['ANALYSIS_PATH'],MuCorrProducer.HighPtmuIDEff_JsonPath.format(period))
        if not MuCorrProducer.initialized:
            headers_dir = os.path.dirname(os.path.abspath(__file__))
            header_path = os.path.join(headers_dir, "mu.h")
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
            ROOT.gInterpreter.ProcessLine(f'::correction::MuCorrProvider::Initialize("{jsonFile_eff}", static_cast<int>({periods[period]}))')
            ROOT.gInterpreter.ProcessLine(f'::correction::HighPtMuCorrProvider::Initialize("{jsonFile_eff_highPt}")')
            MuCorrProducer.period = period
            MuCorrProducer.initialized = True

    def getMuonIDSF(self, df, lepton_legs, isCentral, return_variations):
        SF_branches = []
        sf_sources = MuCorrProducer.muID_SF_Sources + MuCorrProducer.muReco_SF_sources + MuCorrProducer.muIso_SF_Sources
        sf_scales = [central, up, down] if return_variations else [central]
        for source in sf_sources :
            for scale in sf_scales:
                if source == central and scale != central: continue
                if not isCentral and scale!= central: continue
                source_name = MuCorrProducer.muID_SF_Sources_dict[source] if source != central else central
                syst_name = source_name+scale if source != central else 'Central'
                for leg_idx, leg_name in enumerate(lepton_legs):
                    branch_name = f"weight_{leg_name}_MuonID_SF_{syst_name}"
                    branch_central = f"""weight_{leg_name}_MuonID_SF_{source_name+central}"""
                    if source in MuCorrProducer.muReco_SF_sources:
                        df = df.Define(f"{branch_name}_double",f'''HLepCandidate.leg_type[{leg_idx}] == Leg::mu && HLepCandidate.leg_p4[{leg_idx}].pt() >= 10 && HLepCandidate.leg_p4[{leg_idx}].pt() <= 200 ? ::correction::MuCorrProvider::getGlobal().getMuonSF(HLepCandidate.leg_p4[{leg_idx}], Muon_genMatch.at(HLepCandidate.leg_index[{leg_idx}]),Muon_pfRelIso04_all.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tightId.at(HLepCandidate.leg_index[{leg_idx}]),Muon_tkRelIso.at(HLepCandidate.leg_index[{leg_idx}]),Muon_highPtId.at(HLepCandidate.leg_index[{leg_idx}]),::correction::MuCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, "{MuCorrProducer.period}") : 1.''')
                    else:
                        df = df.Define(f"{branch_name}_double", f'''HLepCandidate.leg_type[{leg_idx}] == Leg::mu && HLepCandidate.leg_p4[{leg_idx}].pt() >= 15 ? ::correction::MuCorrProvider::getGlobal().getMuonSF(HLepCandidate.leg_p4[{leg_idx}], Muon_genMatch.at(HLepCandidate.leg_index[{leg_idx}]),Muon_pfRelIso04_all.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tightId.at(HLepCandidate.leg_index[{leg_idx}]),Muon_tkRelIso.at(HLepCandidate.leg_index[{leg_idx}]),Muon_highPtId.at(HLepCandidate.leg_index[{leg_idx}]),::correction::MuCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, "{MuCorrProducer.period}") : 1.''')
                    #print(f"{branch_name}_double")
                    #if scale==central:
                    #    df.Filter(f"{branch_name}_double!=1.").Display({f"{branch_name}_double"}).Print()
                    if scale != central:
                        branch_name_final = branch_name + '_rel'
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double/{branch_central})")
                    else:
                        if source == central:
                            branch_name_final = f"""weight_{leg_name}_MuonID_SF_{central}"""
                        else:
                            branch_name_final = branch_name
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double)")
                    SF_branches.append(branch_name_final)
        return df,SF_branches

########################################################################################################
    #### NO MORE NEEDED (but keeping just in case :D ) ####
    def getHighPtMuonIDSF(self, df, lepton_legs, isCentral, return_variations):
        highPtMuSF_branches = []
        sf_sources =  MuCorrProducer.highPtmuReco_SF_sources + MuCorrProducer.highPtmuID_SF_Sources + MuCorrProducer.highPtmuIso_SF_Sources
        sf_scales = [up, down] if return_variations else []
        for source in sf_sources :
            for scale in [ central ] + sf_scales:
                if source == central and scale != central: continue
                if not isCentral and scale!= central: continue
                source_name = MuCorrProducer.highPtMu_SF_Sources_dict[source] if source != central else central
                syst_name = source_name+scale if source != central else 'Central'
                for leg_idx, leg_name in enumerate(lepton_legs):
                    branch_name = f"weight_{leg_name}_HighPt_MuonID_SF_{syst_name}"
                    #print(branch_name)
                    branch_central = f"""weight_{leg_name}_HighPt_MuonID_SF_{source_name+central}"""
                    df = df.Define(f"{branch_name}_double",f'''HLepCandidate.leg_type[{leg_idx}] == Leg::mu && HLepCandidate.leg_p4[{leg_idx}].pt() >= 120 ? ::correction::HighPtMuCorrProvider::getGlobal().getHighPtMuonSF( HLepCandidate.leg_p4[{leg_idx}], Muon_genMatch.at(HLepCandidate.leg_index[{leg_idx}]), Muon_pfRelIso04_all.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tightId.at(HLepCandidate.leg_index[{leg_idx}]), Muon_highPtId.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tkRelIso.at(HLepCandidate.leg_index[{leg_idx}]),::correction::HighPtMuCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}) : 1.''')
                    #df.Display({f"""{branch_name}_double"""}).Print()
                    #if source in MuCorrProducer.muReco_SF_sources:
                    #    df = df.Define(f"{branch_name}_double",f'''HLepCandidate.leg_type[{leg_idx}] == Leg::mu && HLepCandidate.leg_p4[{leg_idx}].pt() >= 10 && HLepCandidate.leg_p4[{leg_idx}].pt() < 200 ? ::correction::MuCorrProvider::getGlobal().getMuonSF( HLepCandidate.leg_p4[{leg_idx}], Muon_pfRelIso04_all.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tightId.at(HLepCandidate.leg_index[{leg_idx}]),Muon_tkRelIso.at(HLepCandidate.leg_index[{leg_idx}]),Muon_highPtId.at(HLepCandidate.leg_index[{leg_idx}]),::correction::MuCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, "{MuCorrProducer.period}") : 1.''')
                    #else:
                    #    df = df.Define(f"{branch_name}_double", f'''HLepCandidate.leg_type[{leg_idx}] == Leg::mu && HLepCandidate.leg_p4[{leg_idx}].pt() >= 15 && HLepCandidate.leg_p4[{leg_idx}].pt() < 120 ? ::correction::MuCorrProvider::getGlobal().getMuonSF(HLepCandidate.leg_p4[{leg_idx}], Muon_pfRelIso04_all.at(HLepCandidate.leg_index[{leg_idx}]), Muon_tightId.at(HLepCandidate.leg_index[{leg_idx}]),Muon_tkRelIso.at(HLepCandidate.leg_index[{leg_idx}]),Muon_highPtId.at(HLepCandidate.leg_index[{leg_idx}]),::correction::MuCorrProvider::UncSource::{source}, ::correction::UncScale::{scale}, "{MuCorrProducer.period}") : 1.''')
                    #print(f"{branch_name}_double")
                    #if scale==central:
                    #    df.Filter(f"{branch_name}_double!=1.").Display({f"{branch_name}_double"}).Print()
                    if scale != central:
                        branch_name_final = branch_name + '_rel'
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double/{branch_central})")
                    else:
                        if source == central:
                            branch_name_final = f"""weight_{leg_name}_HighPt_MuonID_SF_{central}"""
                        else:
                            branch_name_final = branch_name
                        df = df.Define(branch_name_final, f"static_cast<float>({branch_name}_double)")
                    highPtMuSF_branches.append(branch_name_final)
        return df,highPtMuSF_branches
