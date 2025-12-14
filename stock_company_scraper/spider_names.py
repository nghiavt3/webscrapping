# Tên các Spider (phải khớp với thuộc tính 'name' trong các file Spider tương ứng)

#SPIDER_VIETSTOCK = 'event_table_scraper'
SPIDER_SEAPRIMEXCO = 'event_cat'
SPIDER_VOS = 'event_vos'
SPIDER_TLG = 'event_tlg'
SPIDER_KBC = 'event_kbc'
SPIDER_VDS = 'event_vds'
SPIDER_CII = 'event_cii'
SPIDER_PDR = 'event_pdr'
SPIDER_CEO = 'event_ceo'
SPIDER_QCG = 'event_qcg'
SPIDER_EVG = 'event_evg'
SPIDER_VTP = 'event_vtp'
SPIDER_VIC = 'event_vic'  #fix
SPIDER_VCG = 'event_vcg'
SPIDER_LCG = 'event_lcg'
SPIDER_TPB = 'event_tpb' #fix
SPIDER_TIG = 'event_tig'
SPIDER_MSB = 'event_msb' #fix

SPIDER_ABB = 'event_abb'
SPIDER_AAH = 'event_aah'
SPIDER_C4G = 'event_c4g'
SPIDER_MSR = 'event_msr'
SPIDER_G36 = 'event_g36'
SPIDER_PVX = 'event_pvx'
SPIDER_ACV = 'event_acv'
SPIDER_SBS = 'event_sbs' #fix
SPIDER_HBC = 'event_hbc'
SPIDER_DGT = 'event_dgt'
SPIDER_BVB = 'event_bvb' #chưa khởi tạo file
SPIDER_AAS = 'event_aas' #fix
SPIDER_MZG = 'event_mzg'

SPIDER_DDV = 'event_ddv'
SPIDER_VGI = 'event_vgi'
SPIDER_DRI = 'event_dri'
SPIDER_HNM = 'event_hnm'
SPIDER_VGT = 'event_vgt'#đã fix, tìm thấy api và trả về json
SPIDER_VEA = 'event_vea'
SPIDER_KLB = 'event_klb'
SPIDER_POM = 'event_pom' # fix, render bằng jvscript
SPIDER_MCH = 'event_mch'
SPIDER_OIL = 'event_oil'
SPIDER_FOX = 'event_fox'

SPIDER_FCN = 'event_fcn'
SPIDER_DRH = 'event_drh'
SPIDER_PAC = 'event_pac'
SPIDER_CTD = 'event_ctd'
SPIDER_SZC = 'event_szc'
SPIDER_HHV = 'event_hhv'
SPIDER_VGC = 'event_vgc'
SPIDER_PLC = 'event_plc'
SPIDER_BCC = 'event_bcc'
SPIDER_DHA = 'event_dha' #cần cập nhập lại url mỗi năm 
SPIDER_PC1 = 'event_pc1' # server chặn bot
SPIDER_CTI = 'event_cti'# tìm thấy api và trả về json
SPIDER_IJC = 'event_ijc'
SPIDER_DPG = 'event_dpg'
SPIDER_IDC = 'event_idc'# tìm thấy api và trả về json VIP
SPIDER_SMC = 'event_smc'

SPIDER_KSB = 'event_ksb' # chờ wright tải đầy đủ trang
SPIDER_L14 = 'event_l14'
SPIDER_CMS = 'event_cms'
SPIDER_NO1 = 'event_no1'
SPIDER_THG = 'event_thg'
SPIDER_NBB = 'event_nbb'
SPIDER_CCC = 'event_ccc'
SPIDER_NLG = 'event_nlg'

SPIDER_DXG = 'event_dxg'
SPIDER_NTL = 'event_ntl'
SPIDER_DIG = 'event_dig'
SPIDER_NVL = 'event_nvl'
SPIDER_DTD = 'event_dtd'
SPIDER_HDC = 'event_hdc'
SPIDER_KDH = 'event_kdh'
SPIDER_DC4 = 'event_dc4'
SPIDER_NHA = 'event_nha'
SPIDER_HDG = 'event_hdg'

SPIDER_TCH = 'event_tch'
SPIDER_DXS = 'event_dxs'
SPIDER_CRC = 'event_crc'
SPIDER_GVR = 'event_gvr' #dung wright
SPIDER_SIP = 'event_sip'
SPIDER_PHR = 'event_phr'
SPIDER_TIP = 'event_tip' # tìm dc api và lấy json về
SPIDER_D2D = 'event_d2d' # tìm dc api và lấy json về
SPIDER_NTC = 'event_ntc'
SPIDER_BCM = 'event_bcm'
# Thêm các tên Spider khác vào đây khi bạn mở rộng dự án
ALL_SPIDERS = [ SPIDER_TLG, SPIDER_SEAPRIMEXCO, SPIDER_VOS,SPIDER_KBC,SPIDER_VDS,SPIDER_CII,
               SPIDER_PDR,SPIDER_CEO,SPIDER_QCG,SPIDER_EVG,SPIDER_VTP,SPIDER_VCG,SPIDER_LCG,
               SPIDER_TIG,SPIDER_ABB,SPIDER_AAH,SPIDER_C4G,SPIDER_MSR,SPIDER_PVX,SPIDER_ACV,
               SPIDER_HBC,SPIDER_DGT,SPIDER_MZG,SPIDER_VGI,SPIDER_DRI,SPIDER_HNM,SPIDER_VGT,
               SPIDER_VEA,SPIDER_KLB,SPIDER_MCH,SPIDER_OIL,SPIDER_FOX,SPIDER_FCN,SPIDER_DRH,
               SPIDER_PAC,SPIDER_CTD,SPIDER_SZC,SPIDER_HHV,SPIDER_VGC,SPIDER_PLC,SPIDER_BCC,
               SPIDER_DHA,SPIDER_CTI,SPIDER_IJC,SPIDER_DPG,SPIDER_IDC,SPIDER_SMC,SPIDER_KSB,
               SPIDER_L14,SPIDER_CMS,SPIDER_NO1,SPIDER_THG,SPIDER_NBB,SPIDER_CCC,SPIDER_NLG,
               SPIDER_DXG,SPIDER_NTL,SPIDER_DIG,SPIDER_NVL,SPIDER_DTD,SPIDER_HDC,SPIDER_KDH,
               SPIDER_DC4,SPIDER_NHA,SPIDER_HDG,SPIDER_TCH,SPIDER_DXS,SPIDER_CRC,SPIDER_GVR,
               SPIDER_SIP,SPIDER_PHR,SPIDER_TIP,SPIDER_D2D,SPIDER_NTC,SPIDER_BCM]