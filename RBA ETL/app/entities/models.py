from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import synonym

from sqlalchemy import Table, Column, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

base = automap_base()

foundry_line_mixers = Table(
    'foundry_line_mixers',
    base.metadata,
    Column('foundry_line_pkey', Integer, ForeignKey('foundry_line.pkey'), primary_key=True),
    Column('mixer_pkey', Integer, ForeignKey('foundry_mixer.pkey'), primary_key=True),
    UniqueConstraint('foundry_line_pkey', 'mixer_pkey', name='uq_foundry_line_mixer')
)


class PreparedSand(base):
    __tablename__ = 'preparedsand'

    activeClay = synonym('active_clay')
    wetTensileStrength = synonym('wet_tensile_strength')
    totalClay = synonym('total_clay')
    inertFines = synonym('inert_fines')
    volatileMatter = synonym('volatile_matter')
    specimenWeight = synonym('specimen_weight')
    shearStrength = synonym('shear_strength')
    pHValue = synonym('pH_value')
    shatterIndexNo = synonym('shatter_index_no')
    volatileCombustibleMatter = synonym('volatile_combustible_matter')
    friabilityIndex = synonym('friability_index')
    gfnAfs = synonym('gfn_afs')

    coneJoltTest = synonym('cone_jolt_test')
    tempOfSandBeforeMix = synonym('temp_of_sand_before_mix')
    tempOfSandAfterMix = synonym('temp_of_sand_after_mix')
    ambientTemp = synonym('ambient_temp')
    relativeHumidity = synonym('relative_humidity')
    splitStrength = synonym('split_strength')
    dryCompressionStrength = synonym("dry_compression_strength")


class Rejection(base):
    __tablename__ = 'rejections'

    blowHoleFoundryStage = synonym("blow_hole_foundry_stage")
    blowHoleMachiningStage = synonym("blow_hole_machining_stage")
    brokenMould = synonym("broken_mould")
    burnOn = synonym("burn_on")
    erosionScab = synonym("erosion_scab")
    expansionScab = synonym("expansion_scab")
    explosivePenetration = synonym("explosive_penetration")
    lustrousCarbonDefect = synonym("lustrous_carbon_defect")
    metalPenetration = synonym("metal_penetration")
    pinHoleFoundryStage = synonym("pinhole_foundry_stage")
    pinHoleMachiningStage = synonym("pinhole_machining_stage")
    sandDropInclusionFoundryStage = synonym("sanddrop_inclusion_foundry_stage")
    sandDropInclusionMachiningStage = synonym("sanddrop_inclusion_machining_stage")
    totalSandDropInclusion = synonym("total_sanddrop_inclusion")
    sandFusion = synonym("sand_fusion")
    sandWashErosion = synonym("sand_wash_erosion")
    surfaceRoughness = synonym("surface_roughness")
    swellingOversizeCasting = synonym("swelling_oversize_casting")
    uncategorizedSandRejections = synonym("uncategorized_sand_rejections")
    unpouredMould = synonym("unpoured_mould")

    rejectionQuantity = synonym("rejection_quantity")
    totalQuantityProduced = synonym("total_quantity_produced")

    sandDefect1 = synonym("sand_defect_1")
    sandDefect2 = synonym("sand_defect_2")
    sandDefect3 = synonym("sand_defect_3")
    sandDefect4 = synonym("sand_defect_4")
    sandDefect5 = synonym("sand_defect_5")
    sandDefect6 = synonym("sand_defect_6")
    sandDefect7 = synonym("sand_defect_7")
    sandDefect8 = synonym("sand_defect_8")
    sandDefect9 = synonym("sand_defect_9")
    sandDefect10 = synonym("sand_defect_10")

    def get_synonyms(self):
        return ["blowHoleFoundryStage", "blowHoleMachiningStage", "brokenMould", "burnOn", "erosionScab",
                "expansionScab", "explosivePenetration", "lustrousCarbonDefect", "metalPenetration",
                "pinHoleFoundryStage", "pinHoleMachiningStage", "sandDropInclusionFoundryStage",
                "sandDropInclusionMachiningStage", "totalSandDropInclusion", "sandFusion", "sandWashErosion",
                "surfaceRoughness", "swellingOversizeCasting", "uncategorizedSandRejections", "unpouredMould",
                "rejectionQuantity", "totalQuantityProduced", "sandDefect1", "sandDefect2",
                "sandDefect3", "sandDefect4", "sandDefect5", "sandDefect6", "sandDefect7",
                "sandDefect8", "sandDefect9", "sandDefect10"]

class SMCData(base):
    __tablename__ = 'prepared_sand_extra'


class TempHumidity(base):
    __tablename__ = 'temp_humidity'


class Customer(base):
    __tablename__ = 'customers'

    __mapper_args__ = {
        'exclude_properties': ['show_hide_group_non_admin']
    }


class CustomerSubscription(base):
    __tablename__ = 'customer_subscription'
    __mapper_args__ = {
        'exclude_properties': ['customer']
    }


class Model(base):
    __tablename__ = 'models'

    __mapper_args__ = {
        'exclude_properties': ['model_pkl', 'scale_pkl']
    }


class BlendingModel(base):
    __tablename__ = 'blending_model'
    __mapper_args__ = {
        'exclude_properties': ['foundry_line', 'foundry_line_group']
    }


class BlendingModelTest(base):
    __tablename__ = 'blending_model_test'
    __mapper_args__ = {
        'exclude_properties': ['foundry_line', 'foundry_line_group']
    }

class ScadaData(base):
    __tablename__ = 'scada_data'

class VCSPModel(base):
    __tablename__ = 'vcsp_models'


class DataUploadError(base):
    __tablename__ = 'data_upload_error'


class Measure(base):
    __tablename__ = "measures"


class Property(base):
    __tablename__ = "properties"


class Component(base):
    __tablename__ = "components"
    __mapper_args__ = {
        'exclude_properties': ['devItems']
    }


class FoundryLineGroup(base):
    __tablename__ = "foundry_line_group"

    # foundry_line = relationship("FoundryLine")


class FoundryLine(base):
    __tablename__ = "foundry_line"

    __mapper_args__ = {
        'exclude_properties': ['heat_no_validation_limit', 'include_non_model_params', 'base_line_cfg_json']
    }


class FoundryLineGroupComponent(base):
    __tablename__ = "foundry_line_group_component"


class ConsumptionBooking(base):
    __tablename__ = "consumption_booking"


class SandFusion(base):
    __tablename__ = "sand_fusion"


class SubscriptionDetail(base):
    __tablename__ = "subscription_detail"

    __mapper_args__ = {
        'exclude_properties': ['customer']
    }