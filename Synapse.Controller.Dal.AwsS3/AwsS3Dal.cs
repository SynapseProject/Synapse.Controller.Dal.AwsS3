﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Suplex.Security;

using Synapse.Core;
using Synapse.Core.Utilities;
using Synapse.Services;
using Synapse.Services.Controller.Dal;

using zf = Zephyr.Filesystem;


//namespace Synapse.Services.Controller.Dal { }
public partial class AwsS3Dal : IControllerDal
{
    static readonly string CurrentPath = $"{Path.GetDirectoryName( typeof( AwsS3Dal ).Assembly.Location )}";

    private zf.AwsClient _awsClient;
    private string _bucketName = null;

    string _planPath = null;
    string _histPath = null;
    string _splxPath = null;

    SuplexDal _splxDal = null;

    //this is a stub feature
    static long PlanInstanceIdCounter = DateTime.Now.Ticks;


    public AwsS3Dal()
    {
    }

    internal AwsS3Dal(string basePath, bool processPlansOnSingleton = false, bool processActionsOnSingleton = true) : this()
    {
        if( string.IsNullOrWhiteSpace( basePath ) )
            basePath = CurrentPath;

        _planPath = $"{basePath}\\Plans\\";
        _histPath = $"{basePath}\\History\\";
        _splxPath = $"{basePath}\\Security\\";

        EnsurePaths();

        ProcessPlansOnSingleton = processPlansOnSingleton;
        ProcessActionsOnSingleton = processActionsOnSingleton;

        LoadSuplex();
    }


    public object GetDefaultConfig()
    {
        return new AwsS3DalConfig();
    }


    public Dictionary<string, string> Configure(ISynapseDalConfig conifg)
    {
        if( conifg != null )
        {
            string s = YamlHelpers.Serialize( conifg.Config );
            AwsS3DalConfig fsds = YamlHelpers.Deserialize<AwsS3DalConfig>( s );

            _awsClient = new zf.AwsClient( "", "", null );

            _bucketName = fsds.BucketName;

            _planPath = fsds.PlanFolderPath;
            _histPath = fsds.HistoryFolderPath;
            _splxPath = fsds.Security.FilePath;

            EnsurePaths();

            ProcessPlansOnSingleton = fsds.ProcessPlansOnSingleton;
            ProcessActionsOnSingleton = fsds.ProcessActionsOnSingleton;

            LoadSuplex();

            if( _splxDal == null && fsds.Security.IsRequired )
                throw new Exception( $"Security is required.  Could not load security file: {_splxPath}." );

            if( _splxDal != null )
            {
                _splxDal.LdapRoot = conifg.LdapRoot;
                _splxDal.GlobalExternalGroupsCsv = fsds.Security.GlobalExternalGroupsCsv;
            }
        }
        else
        {
            ConfigureDefaults();
        }

        string name = nameof( AwsS3Dal );
        Dictionary<string, string> props = new Dictionary<string, string>
        {
            { name, CurrentPath },
            { $"{name} Bucket Name", _bucketName },
            { $"{name} Plan path", _planPath },
            { $"{name} History path", _histPath },
            { $"{name} Security path", _splxPath }
        };
        return props;
    }

    internal void ConfigureDefaults()
    {
        _planPath = $"{CurrentPath}\\Plans\\";
        _histPath = $"{CurrentPath}\\History\\";
        _splxPath = $"{CurrentPath}\\Security\\";

        EnsurePaths();

        ProcessPlansOnSingleton = false;
        ProcessActionsOnSingleton = true;

        LoadSuplex();
    }

    void EnsurePaths()
    {
        //GetFullPath tests below validate the paths are /complete/ paths.  IsPathRooted returns 'true'
        //in a few undesriable cases

        if( Path.GetFullPath( _planPath ) != _planPath )
            _planPath = Utilities.PathCombine( CurrentPath, _planPath, "\\" );

        if( Path.GetFullPath( _histPath ) != _histPath )
            _histPath = Utilities.PathCombine( CurrentPath, _histPath, "\\" );

        if( Path.GetFullPath( _splxPath ) != _splxPath )
            _splxPath = Utilities.PathCombine( CurrentPath, _splxPath, "\\" );

        zf.AwsS3ZephyrDirectory s3zd = new zf.AwsS3ZephyrDirectory( _awsClient );

        s3zd.FullName = _planPath;
        if( !s3zd.Exists )
            s3zd.Create();

        s3zd.FullName = _histPath;
        if( !s3zd.Exists )
            s3zd.Create();
    }

    void LoadSuplex()
    {
        string splx = Utilities.PathCombine( _splxPath, "security.splx" );
        zf.AwsS3ZephyrFile s3splx = new zf.AwsS3ZephyrFile( _awsClient, splx );
        if( s3splx.Exists )
        {
            string storeData = s3splx.ReadAllText();
            _splxDal = new SuplexDal();
            _splxDal.LoadStoreData( storeData );
        }
    }


    public bool ProcessPlansOnSingleton { get; set; }
    public bool ProcessActionsOnSingleton { get; set; }
}