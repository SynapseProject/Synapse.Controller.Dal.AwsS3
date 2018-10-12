using System;
using System.Collections.Generic;

namespace Synapse.Services.Controller.Dal
{
    public class AwsS3DalConfig
    {
        public string AwsAccessKey { get; set; }
        public string AwsSecretAccessKey { get; set; }
        public string DefaultBucketName { get; set; }
        public string PlanFolderPath { get; set; } = "Plans";
        public string HistoryFolderPath { get; set; } = "History";
        public bool ProcessPlansOnSingleton { get; set; } = false;
        public bool ProcessActionsOnSingleton { get; set; } = true;

        public SecurityConfig Security { get; set; } = new SecurityConfig();
    }

    public class SecurityConfig
    {
        public string FilePath { get; set; }= "Security";
        public bool IsRequired { get; set; } = false;
        public bool ValidateSignature { get; set; } = false;
        public string SignaturePublicKeyFile { get; set; }
        public string GlobalExternalGroupsCsv { get; set; } = "Everyone";
    }
}