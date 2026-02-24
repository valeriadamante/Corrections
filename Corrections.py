import os
import itertools

from .CorrectionsCore import *
from FLAF.RunKit.run_tools import ps_call


def findLibLocation(lib_name, first_guess=None):
    paths_to_check = []
    if first_guess is not None:
        paths_to_check.append(first_guess)
    other_paths = os.environ.get("LD_LIBRARY_PATH", "").split(":")
    paths_to_check.extend(other_paths)
    full_lib_name = f"lib{lib_name}.so"
    for path in paths_to_check:
        lib_path = os.path.join(path, full_lib_name)
        if os.path.exists(lib_path):
            return lib_path
    raise RuntimeError(f"Library {lib_name} not found.")


class Corrections:
    _global_instance = None
    _corr_lib_loaded = False

    @staticmethod
    def initializeGlobal(load_corr_lib=False, **kwargs):
        if Corrections._global_instance is not None:
            print(
                f"WARNING: Global instance of Corrections was already initialized. Overwriting it.",
                file=sys.stderr,
            )

        if load_corr_lib and not Corrections._corr_lib_loaded:
            returncode, output, err = ps_call(
                ["correction", "config", "--cflags", "--ldflags"],
                catch_stdout=True,
                decode=True,
                verbose=0,
            )
            params = output.split(" ")
            lib_path = None
            for param in params:
                if param.startswith("-I"):
                    ROOT.gInterpreter.AddIncludePath(param[2:].strip())
                elif param.startswith("-L"):
                    lib_path = param[2:].strip()
                elif param.startswith("-l"):
                    lib_name = param[2:].strip()

            # ROOT.gInterpreter.AddIncludePath(os.environ['FLAF_ENVIRONMENT_PATH']+"/include")
            corr_lib = findLibLocation(lib_name, lib_path)
            ROOT.gSystem.Load(corr_lib)
            Corrections._corr_lib_loaded = True

        Corrections._global_instance = Corrections(**kwargs)

    @staticmethod
    def getGlobal():
        if Corrections._global_instance is None:
            raise RuntimeError("Global instance is not initialized")
        return Corrections._global_instance

    def __init__(
        self,
        *,
        global_params,
        stage,
        dataset_name,
        dataset_cfg,
        process_name,
        process_cfg,
        processors,
        isData,
        trigger_class,
    ):
        self.global_params = global_params
        self.dataset_name = dataset_name
        self.dataset_cfg = dataset_cfg
        self.process_name = process_name
        self.process_cfg = process_cfg
        self.processors = processors
        self.isData = isData
        self.trigger_dict = trigger_class.trigger_dict if trigger_class else {}

        self.period = global_params["era"]
        self.stage = stage

        self.to_apply = {}
        correction_origins = {}
        for cfg_name, cfg in [
            ("dataset", dataset_cfg),
            ("process", process_cfg),
            ("global", global_params),
        ]:
            if not cfg:
                continue
            for corr_name, corr_params in cfg.get("corrections", {}).items():
                if "stage" in corr_params and "stages" in corr_params:
                    raise RuntimeError(
                        f"correction {corr_name} in {cfg_name} has both 'stage' and 'stages' defined. Please use only one of them."
                    )
                corr_stages = corr_params.get("stages", [])
                if "stage" in corr_params:
                    corr_stages.append(corr_params["stage"])
                if stage not in corr_stages:
                    continue
                if corr_name not in self.to_apply:
                    self.to_apply[corr_name] = corr_params
                    correction_origins[corr_name] = cfg_name
                else:
                    print(
                        f"Warning: correction {corr_name} is already defined in {correction_origins[corr_name]}. Skipping definition from {cfg_name}",
                        file=sys.stderr,
                    )
        if len(self.to_apply) > 0:
            print(
                f'Corrections to apply: {", ".join(self.to_apply.keys())}',
                file=sys.stderr,
            )

        self.xs_db_ = None
        self.tau_ = None
        self.met_ = None
        self.trg_ = None
        self.btag_ = None
        self.pu_ = None
        self.mu_ = None
        self.muScaRe_ = None
        self.ele_ = None
        self.puJetID_ = None
        self.jet_ = None
        self.fatjet_ = None
        self.Vpt_ = None
        self.JetVetoMap_ = None

    @property
    def xs_db(self):
        if self.xs_db_ is None:
            from FLAF.Common.CrossSectionDB import CrossSectionDB

            self.xs_db_ = CrossSectionDB.Load(
                os.environ["ANALYSIS_PATH"],
                self.global_params["crossSectionsFile"],
            )
        return self.xs_db_

    @property
    def pu(self):
        if self.pu_ is None:
            from .pu import puWeightProducer

            self.pu_ = puWeightProducer(period=period_names[self.period])
        return self.pu_

    @property
    def Vpt(self):
        if self.Vpt_ is None:
            from .Vpt import VptCorrProducer

            self.Vpt_ = VptCorrProducer(self.to_apply["Vpt"]["type"], self.period)
        return self.Vpt_

    @property
    def JetVetoMap(self):
        if self.JetVetoMap_ is None:
            from .JetVetoMap import JetVetoMapProvider

            self.JetVetoMap_ = JetVetoMapProvider(self.period)
        return self.JetVetoMap_

    @property
    def tau(self):
        if self.tau_ is None:
            from .tau import TauCorrProducer

            self.tau_ = TauCorrProducer(
                period=self.period,
                config=self.global_params,
                columns=self.to_apply.get("tauID", {}).get("columns", {}),
            )
        return self.tau_

    @property
    def jet(self):
        if self.jet_ is None:
            from .jet import JetCorrProducer

            self.jet_ = JetCorrProducer(
                period_names[self.period], self.isData, self.dataset_name
            )
        return self.jet_

    @property
    def fatjet(self):
        if self.fatjet_ is None:
            from .fatjet import FatJetCorrProducer

            self.fatjet_ = FatJetCorrProducer(
                period=period_names[self.period],
                ana=self.to_apply.get("fatjet", {}).get("ana", ""),
                tagger=self.to_apply.get("fatjet", {}).get("tagger", ""),
                fatjetName=self.to_apply.get("fatjet", {}).get("fatJetName", ""),
                isData=self.isData,
            )
        return self.fatjet_

    @property
    def btag(self):
        if self.btag_ is None:
            from .btag import bTagCorrProducer

            params = self.to_apply["btag"]
            self.btag_ = bTagCorrProducer(
                period=period_names[self.period],
                jetCollection=params["jetCollection"],
                tagger=params["tagger"],
                loadEfficiency=params.get("loadEfficiency", False),
                useSplitJes=params.get("useSplitJes", False),
                wantShape=params.get("wantShape", True),
            )
        return self.btag_

    @property
    def met(self):
        if self.met_ is None:
            from .met import METCorrProducer

            self.met_ = METCorrProducer()
        return self.met_

    @property
    def mu(self):
        if self.mu_ is None:
            from .mu import MuCorrProducer

            self.mu_ = MuCorrProducer(
                era=self.period, columns=self.to_apply["mu"].get("columns", {})
            )
        return self.mu_

    @property
    def muScaRe(self):
        if self.muScaRe_ is None:
            from .MuonScaRe_corr import MuonScaReCorrProducer

            self.muScaRe_ = MuonScaReCorrProducer(
                period_names[self.period],
                self.isData,
                self.to_apply["muScaRe"].get("mu_pt_for_ScaReApplication", "pt_nano"),
            )
        return self.muScaRe_

    @property
    def ele(self):
        if self.ele_ is None:
            from .electron import EleCorrProducer

            self.ele_ = EleCorrProducer(
                period=period_names[self.period],
                columns=self.to_apply.get("ele", {}).get("columns", {}),
                isData=self.isData,
            )
        return self.ele_

    @property
    def puJetID(self):
        if self.puJetID_ is None:
            from .puJetID import puJetIDCorrProducer

            self.puJetID_ = puJetIDCorrProducer(period_names[self.period])
        return self.puJetID_

    @property
    def trg(self):
        if self.trg_ is None:
            if self.period.split("_")[0].startswith("Run3"):
                from .triggersRun3 import TrigCorrProducer
            else:
                from .triggers import TrigCorrProducer
            self.trg_ = TrigCorrProducer(
                period_names[self.period], self.global_params, self.trigger_dict
            )
        return self.trg_

    def applyScaleUncertainties(self, df, ana_reco_objects):
        source_dict = {central: []}
        if "tauES" in self.to_apply and not self.isData:
            df, source_dict = self.tau.getES(df, source_dict)
        if "eleES" in self.to_apply:
            df, source_dict = self.ele.getES(df, source_dict)
        if "JEC" in self.to_apply or "JER" in self.to_apply:
            apply_jes = "JEC" in self.to_apply and not self.isData
            apply_jer = "JER" in self.to_apply and not self.isData
            apply_jet_horns_fix_ = (
                "JER" in self.to_apply
                and self.to_apply["JER"].get("apply_jet_horns_fix", False)
                and not self.isData
            )
            df, source_dict = self.jet.getP4Variations(
                df, source_dict, apply_jer, apply_jes, apply_jet_horns_fix_
            )
        if "muScaRe" in self.to_apply:
            df, source_dict = (
                self.muScaRe.getP4Variations(df, source_dict)
                if self.stage == "AnaTuple"
                else self.muScaRe.getP4VariationsForLegs(df)
            )
        if (
            "tauES" in self.to_apply
            or "JEC" in self.to_apply
            or "JER" in self.to_apply
            or "eleES" in self.to_apply
            or "muScaRe" in self.to_apply
        ):
            df, source_dict = self.met.getMET(
                df, source_dict, self.global_params["met_type"]
            )

        syst_dict = {}
        for source, source_objs in source_dict.items():
            for scale in getScales(source):
                syst_name = getSystName(source, scale)
                syst_dict[syst_name] = (source, scale)
                for obj in ana_reco_objects:
                    if obj not in source_objs:
                        suffix = (
                            "Central"
                            if f"{obj}_p4_Central" in df.GetColumnNames()
                            else "nano"
                        )
                        # suffix = 'nano'
                        if (
                            obj == "boostedTau"
                            and "{obj}_p4_{suffix}" not in df.GetColumnNames()
                        ):
                            continue
                        if f"{obj}_p4_{syst_name}" not in df.GetColumnNames():
                            # print(
                            #     f"Defining nominal {obj}_p4_{syst_name} as {obj}_p4_{suffix}"
                            # )
                            df = df.Define(
                                f"{obj}_p4_{syst_name}", f"{obj}_p4_{suffix}"
                            )
        return df, syst_dict

    def defineCrossSection(self, df, crossSectionBranch):
        xs_processor_names = []
        xs_results = []
        for p_name, proc in self.processors.items():
            if hasattr(proc, "onAnaTuple_defineCrossSection"):
                xs_processor_names.append(p_name)
        if len(xs_processor_names) == 0:
            raise RuntimeError(
                "No processor implements onAnaTuple_defineCrossSection method"
            )

        print(
            f"Stitching detected for {self.dataset_name}. Processors: {xs_processor_names}"
        )
        for p_name in xs_processor_names:
            proc = self.processors[p_name]
            df = proc.onAnaTuple_defineCrossSection(
                df,
                f"{crossSectionBranch}_{p_name}",
                self.xs_db,
                self.dataset_name,
                self.dataset_cfg,
            )
            xs_results.append(f"{crossSectionBranch}_{p_name}")
        return df, xs_results

    def defineDenominator(self, df, denomBranch, unc_source, unc_scale, ana_caches):
        denom_processor_names = []
        for p_name, proc in self.processors.items():
            if hasattr(proc, "onAnaTuple_defineDenominator"):
                denom_processor_names.append(p_name)
        if len(denom_processor_names) == 0:
            raise RuntimeError(
                "No processor implements onAnaTuple_defineDenominator method"
            )
        if len(denom_processor_names) > 1:
            raise RuntimeError(
                "Multiple processors implement onAnaTuple_defineDenominator method. Not supported."
            )
        p_name = denom_processor_names[0]
        print(
            f'Using processor "{p_name}" to define denominator for dataset "{self.dataset_name}"'
        )
        if len(ana_caches) > 1:
            print(
                f"Available ana_caches for denominator calculation: {list(ana_caches.keys())}"
            )
        denom_processor = self.processors[p_name]
        return denom_processor.onAnaTuple_defineDenominator(
            df,
            denomBranch,
            p_name,
            self.dataset_name,
            unc_source,
            unc_scale,
            ana_caches,
        )

    def getNormalisationCorrections(
        self,
        df,
        *,
        lepton_legs,
        offline_legs,
        trigger_names,
        unc_source,
        unc_scale,
        ana_caches,
        return_variations=True,
        use_genWeight_sign_only=True,
    ):
        isCentral = unc_source == central
        all_weights = []
        lumi_weight_name = "weight_lumi"
        if "lumi" in self.to_apply:
            lumi = self.global_params["luminosity"]
            df = df.Define(lumi_weight_name, f"float({lumi})")
            all_weights.append(lumi_weight_name)

        crossSectionBranch = "weight_xs"
        if "xs" in self.to_apply:
            df, xs_branches = self.defineCrossSection(df, crossSectionBranch)
            all_weights.extend(xs_branches)

        gen_weight_name = "weight_gen"
        if "gen" in self.to_apply:
            genWeight_def = (
                "std::copysign<float>(1.f, genWeight)"
                if use_genWeight_sign_only
                else "genWeight"
            )
            df = df.Define(gen_weight_name, genWeight_def)
            all_weights.append(gen_weight_name)

        shape_weights_dict = {(central, central): []}
        if "pu" in self.to_apply:
            pu_enabled = self.to_apply["pu"].get("enabled", {}).get(self.stage, True)
            df, weight_pu_branches = self.pu.getWeight(
                df,
                shape_weights_dict=shape_weights_dict,
                return_variations=return_variations and isCentral,
                return_list_of_branches=True,
                enabled=pu_enabled,
            )
            all_weights.extend(weight_pu_branches)

        if "base" in self.to_apply:
            weight_xs_branches = [
                w for w in df.GetColumnNames() if crossSectionBranch in w
            ]
            for (
                shape_unc_source,
                shape_unc_scale,
            ), shape_weights in shape_weights_dict.items():
                shape_unc_name = getSystName(shape_unc_source, shape_unc_scale)
                denomBranch = f"__denom_{shape_unc_name}"
                df = self.defineDenominator(
                    df, denomBranch, shape_unc_source, shape_unc_scale, ana_caches
                )
                shape_weights_product = (
                    " * ".join(shape_weights) if len(shape_weights) > 0 else "1.0"
                )

                def AddWeightToAllWeights(
                    df, all_weights, weight_name_central, xs_branch
                ):
                    if shape_unc_name == central:
                        weight_name = weight_name_central
                        weight_out_name = weight_name
                    else:
                        weight_name = f"{weight_name_central}_{shape_unc_name}"
                        weight_out_name = f"{weight_name}_rel"

                    weight_formula = f"{gen_weight_name} * {lumi_weight_name} * {xs_branch} * {shape_weights_product} / {denomBranch}"

                    df = df.Define(weight_name, f"static_cast<float>({weight_formula})")

                    if shape_unc_name != central:
                        df = df.Define(
                            weight_out_name,
                            f"static_cast<float>({weight_name}/{weight_name_central})",
                        )

                    all_weights.append(weight_out_name)

                    return df, all_weights

                weight_name_central = "weight_base"
                if (
                    len(weight_xs_branches) > 1
                ):  # it HANDLES ONLY THE CASES OF [STITCHER, DEFAULT] FOR THE MOMENT!!
                    for xs_branch in weight_xs_branches:
                        xs_branch_split = xs_branch.split("_")
                        xs_branch_suffix = xs_branch_split[-1]
                        if (
                            xs_branch_suffix == "default"
                        ):  # for the default we have the weight_base_ds, whereas for the stitcher we have the weight_base
                            weight_name_central = "weight_base_ds"
                        else:
                            weight_name_central = "weight_base"
                        df, all_weights = AddWeightToAllWeights(
                            df, all_weights, weight_name_central, xs_branch
                        )
                else:

                    df, all_weights = AddWeightToAllWeights(
                        df, all_weights, weight_name_central, weight_xs_branches[0]
                    )
                    if (
                        f"weight_base_ds" not in df.GetColumnNames()
                    ):  # to handle cases where there is no difference between default and stitcher
                        df = df.Define("weight_base_ds", "weight_base")
                    else:
                        print(
                            f"attention, although there is only 1 XS branch {weight_xs_branches}, the weight_base_ds is already defined, not normal behaviour."
                        )
                    all_weights.append("weight_base_ds")
                print(all_weights)

        if "Vpt" in self.to_apply:
            df, Vpt_SF_branches = self.Vpt.getSF(df, isCentral, return_variations)
            all_weights.extend(Vpt_SF_branches)
            df, Vpt_DYw_branches = self.Vpt.getDYSF(df, isCentral, return_variations)
            all_weights.extend(Vpt_DYw_branches)
        if "tauID" in self.to_apply:
            df, tau_SF_branches = self.tau.getSF(
                df, lepton_legs, isCentral, return_variations
            )
            all_weights.extend(tau_SF_branches)
        if "btag" in self.to_apply:
            btag_sf_mode = self.to_apply["btag"]["modes"][self.stage]
            if btag_sf_mode in ["shape", "wp"]:
                if btag_sf_mode == "shape":
                    df, bTagSF_branches = self.btag.getBTagShapeSF(
                        df, unc_source, unc_scale, isCentral, return_variations
                    )
                else:
                    df, bTagSF_branches = self.btag.getBTagWPSF(
                        df, isCentral and return_variations, isCentral
                    )
                all_weights.extend(bTagSF_branches)
            elif btag_sf_mode != "none":
                raise RuntimeError(
                    f"btag mode {btag_sf_mode} not recognized. Supported modes are 'shape', 'wp' and 'none'."
                )
        if "mu" in self.to_apply:
            if self.mu.low_available:
                df, lowPtmuID_SF_branches = self.mu.getLowPtMuonIDSF(
                    df, lepton_legs, isCentral, return_variations
                )
                all_weights.extend(lowPtmuID_SF_branches)
            if self.mu.med_available:
                df, muID_SF_branches = self.mu.getMuonIDSF(
                    df, lepton_legs, isCentral, return_variations
                )
                all_weights.extend(muID_SF_branches)
            if self.mu.high_available:
                df, highPtmuID_SF_branches = self.mu.getHighPtMuonIDSF(
                    df, lepton_legs, isCentral, return_variations
                )
                all_weights.extend(highPtmuID_SF_branches)
        if "ele" in self.to_apply:
            df, eleID_SF_branches = self.ele.getIDSF(
                df, lepton_legs, isCentral, return_variations
            )
            all_weights.extend(eleID_SF_branches)
        if "puJetID" in self.to_apply:
            df, puJetID_SF_branches = self.puJetID.getPUJetIDEff(
                df, isCentral, return_variations
            )
            all_weights.extend(puJetID_SF_branches)
        if "trigger" in self.to_apply:
            mode = self.to_apply["trigger"]["mode"]
            if mode == "SF":
                df, trg_SF_branches = self.trg.getSF(
                    df,
                    trigger_names,
                    lepton_legs,
                    isCentral and return_variations,
                    isCentral,
                    extraFormat=self.to_apply["trigger"].get("extraFormat", {}),
                )
                all_weights.extend(trg_SF_branches)
            elif mode == "efficiency":
                df, trg_SF_branches = self.trg.getEff(
                    df, trigger_names, offline_legs, self.trigger_dict
                )
                all_weights.extend(trg_SF_branches)
            else:
                raise RuntimeError(
                    f"Trigger correction mode {mode} not recognized. Supported modes are 'SF' and 'efficiency'."
                )
        if "fatjet" in self.to_apply:
            # bbWW fatjet corrections taken from here
            # https://indico.cern.ch/event/1573622/#6-updates-on-ak8-calibration-f
            df, fatjet_SF_branches = self.fatjet.getSF(df, isCentral, return_variations)
            all_weights.extend(fatjet_SF_branches)

        return df, all_weights


# amcatnlo problem
# https://cms-talk.web.cern.ch/t/correct-way-to-stitch-lo-w-jet-inclusive-and-jet-binned-samples/17651/3
# https://cms-talk.web.cern.ch/t/stitching-fxfx-merged-njet-binned-samples/16751/7
