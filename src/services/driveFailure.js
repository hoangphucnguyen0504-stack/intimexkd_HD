'use strict';
// Only explicit credential/account/destination failures allow switching providers.
function driveFailure(error){
 const detail=error?.response?.data?.error,status=Number(error?.response?.status||error?.code);
 const reasons=[...(Array.isArray(detail?.errors)?detail.errors:[]),...(Array.isArray(detail?.details)?detail.details:[])].map(x=>x?.reason);
 const oauth=typeof detail==='string'?detail:error?.code;
 if(status===429||status>=500||reasons.includes('rateLimitExceeded')||reasons.includes('dailyLimitExceeded'))return null;
 if(['invalid_grant','invalid_token','DRIVE_TOKEN_INVALID','DRIVE_AUTH_FAILED'].includes(oauth))return 'invalid_grant';
 if(['invalid_client','unauthorized_client','deleted_client','DRIVE_CLIENT_INVALID','DRIVE_API_DISABLED'].includes(oauth)||reasons.some(x=>['accessNotConfigured','SERVICE_DISABLED'].includes(x)))return 'invalid_client';
 if(oauth==='DRIVE_USER_RATE_LIMIT'||(status===403&&reasons.includes('userRateLimitExceeded')))return 'userRateLimitExceeded';
 if(oauth==='DRIVE_STORAGE_FULL'||reasons.includes('storageQuotaExceeded'))return 'storageQuotaExceeded';
 if(oauth==='DRIVE_ACCOUNT_DISABLED'||reasons.includes('accountDisabled'))return 'accountDisabled';
 if(['DRIVE_FOLDER_INVALID','DRIVE_FOLDER_NOT_FOUND','DRIVE_SCOPE_MISSING','IMAGE_DRIVE_FOLDER_REQUIRED','DRIVE_PERMISSION_DENIED'].includes(oauth)||reasons.some(x=>['insufficientFilePermissions','insufficientPermissions','ACCESS_TOKEN_SCOPE_INSUFFICIENT'].includes(x)))return 'permissionDenied';
 return null;
}
module.exports={driveFailure};
