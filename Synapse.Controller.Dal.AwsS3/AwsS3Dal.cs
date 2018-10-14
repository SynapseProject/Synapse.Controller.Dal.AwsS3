using System;
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
    private string _region = null;

    string _planPath = null;
    string _histPath = null;
    bool _histAsJson = false;
    bool _histAsFormattedJson = false;
    string _histExt = ".yaml";
    string _splxPath = null;

    SuplexDal _splxDal = null;

    //this is a stub feature
    static long PlanInstanceIdCounter = DateTime.Now.Ticks;


    public AwsS3Dal()
    {
    }

    internal AwsS3Dal(string basePath, string accessKey, string secretAccessKey,
        bool processPlansOnSingleton = false, bool processActionsOnSingleton = true) : this()
    {
        if( string.IsNullOrWhiteSpace( basePath ) )
            basePath = CurrentPath;

        _region = Amazon.RegionEndpoint.USEast1.ToString();
        _awsClient = new zf.AwsClient( accessKey, secretAccessKey, Amazon.RegionEndpoint.USEast1 );

        _bucketName = basePath;
        _planPath = $"{basePath}/Plans/";
        _histPath = $"{basePath}/History/";
        _splxPath = $"{basePath}/Security/";

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

            _region = Amazon.RegionEndpoint.USEast1.ToString();

            if( string.IsNullOrWhiteSpace( fsds.AwsAccessKey ) || string.IsNullOrWhiteSpace( fsds.AwsSecretAccessKey ) )
                _awsClient = new zf.AwsClient( Amazon.RegionEndpoint.USEast1 );
            else
                _awsClient = new zf.AwsClient( fsds.AwsAccessKey, fsds.AwsSecretAccessKey, Amazon.RegionEndpoint.USEast1 );

            _bucketName = fsds.DefaultBucketName;

            _planPath = fsds.PlanFolderPath;
            _histPath = fsds.HistoryFolderPath;
            _histAsJson = fsds.WriteHistoryAs == HistorySerializationFormat.FormattedJson || fsds.WriteHistoryAs == HistorySerializationFormat.CompressedJson;
            _histAsFormattedJson = fsds.WriteHistoryAs == HistorySerializationFormat.FormattedJson;
            _histExt = _histAsJson ? ".json" : ".yaml";
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
            { $"{name} AWS Region", _region },
            { $"{name} S3 Default Bucket", _bucketName },
            { $"{name} Plan path", _planPath },
            { $"{name} History path", _histPath },
            { $"{name} Security path", _splxPath }
        };
        return props;
    }

    internal void ConfigureDefaults()
    {
        _awsClient = new zf.AwsClient( Amazon.RegionEndpoint.USEast1 );

        _bucketName = "s3://need_a_valid_bucket";
        _planPath = $"{_bucketName}/Plans/";
        _histPath = $"{_bucketName}/History/";
        _splxPath = $"{_bucketName}/Security/";

        EnsurePaths();

        ProcessPlansOnSingleton = false;
        ProcessActionsOnSingleton = true;

        LoadSuplex();
    }

    void EnsurePaths()
    {
        const string s3 = "s3://";

        if( !_planPath.StartsWith( s3, StringComparison.OrdinalIgnoreCase ) )
            _planPath = UtilitiesPathCombine( _bucketName, _planPath );

        if( !_histPath.StartsWith( s3, StringComparison.OrdinalIgnoreCase ) )
            _histPath = UtilitiesPathCombine( _bucketName, _histPath );

        if( !_splxPath.StartsWith( s3, StringComparison.OrdinalIgnoreCase ) )
            _splxPath = UtilitiesPathCombine( _bucketName, _splxPath );

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
        string splxFile = DirectoryGetFile( _splxPath, "security.splx", throwFileNotFoundException: false );
        if( splxFile != null )
        {
            zf.AwsS3ZephyrFile s3splx = new zf.AwsS3ZephyrFile( _awsClient, splxFile );
            string storeData = s3splx.ReadAllText();
            _splxDal = new SuplexDal();
            _splxDal.LoadStoreData( storeData );
        }
    }


    public bool ProcessPlansOnSingleton { get; set; }
    public bool ProcessActionsOnSingleton { get; set; }
}