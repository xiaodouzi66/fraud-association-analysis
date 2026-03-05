select count(1) from  tmp_zjj_risk_propagation_path_0227_py_v0;
-- 2239 


select a.target_policy
,a.propagation_level
,a.risk_score
,b.insured_cert_no_md5
,b.insure_date
from tmp_zjj_risk_propagation_path_0227_py_v0 a 
left join 
(
    select policy_no
    ,md5(insured_cert_no) as insured_cert_no_md5
    ,insure_date
    from za_ha_prd.cdm_ha_policy_wt_dd 
    where pt=max_pt("za_ha_prd.cdm_ha_policy_wt_dd")
    and pt2='3' --个险
)b 
on a.target_policy=b.policy_no



