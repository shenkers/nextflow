/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.cloud.aws.util

import com.amazonaws.services.s3.model.CannedAccessControlList
import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.cloud.aws.batch.AwsOptions
import nextflow.executor.BashFunLib

/**
 * AWS S3 helper class
 */
@CompileStatic
class S3BashLib extends BashFunLib<S3BashLib> {

    private String storageClass = 'STANDARD'
    private String storageEncryption = ''
    private String storageKmsKeyId = ''
    private String debug = ''
    private String cli = 'aws'
    private String retryMode
    private String s5cmdPath
    private String acl = ''
    private String requesterPays = ''
    private String cache = ''

    S3BashLib withCliPath(String cliPath) {
        if( cliPath )
            this.cli = cliPath
        return this
    }

    S3BashLib withRetryMode(String value) {
        if( value )
            retryMode = value
        return this
    }

    S3BashLib withDebug(Boolean  value) {
        this.debug = value ? '--debug ' : ''
        return this
    }

    S3BashLib withStorageClass(String value) {
        if( value )
            this.storageClass = value
        return this
    }

    S3BashLib withStorageEncryption(String value) {
        if( value )
            this.storageEncryption = value ? "--sse $value " : ''
        return this
    }

    S3BashLib withStorageKmsKeyId(String value) {
        if( value )
            this.storageKmsKeyId = value ? "--sse-kms-key-id $value " : ''
        return this
    }

    S3BashLib withS5cmdPath(String value) {
        this.s5cmdPath = value
        return this
    }

    S3BashLib withAcl(CannedAccessControlList value) {
        if( value )
            this.acl = "--acl $value "
        return this
    }

    S3BashLib withRequesterPays(Boolean value) {
        this.requesterPays = value ? "--request-payer requester " : ''
        return this
    }

    S3BashLib withCache(String cache) {
        this.cache = cache
        return this
    }

    protected String retryEnv() {
        if( !retryMode )
            return ''
        """
        # aws cli retry config
        export AWS_RETRY_MODE=${retryMode} 
        export AWS_MAX_ATTEMPTS=${maxTransferAttempts}
        """.stripIndent().rightTrim()
    }

    /**
     * Implement S3 upload/download helper using `aws s3` CLI tool
     *
     * @return The Bash script implementing the S3 helper functions
     */
    protected String s3Lib() {
        """
        # aws helper
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              $cli s3 cp --only-show-errors ${debug}${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass - "\$s3path"
            elif [[ -d "\$name" ]]; then
              $cli s3 cp --only-show-errors --recursive ${debug}${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name" "\$s3path/\$name"
            else
              $cli s3 cp --only-show-errors ${debug}${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name" "\$s3path/\$name"
            fi
        }
        
        nxf_s3_download() {
            local source=\$1
            local target=\$2
            local file_name=\$(basename \$1)
            local is_dir=\$($cli s3 ls \$source | grep -F "PRE \${file_name}/" -c)
            if [[ \$is_dir == 1 ]]; then
                $cli s3 cp --only-show-errors --recursive "\$source" "\$target"
            else 
                $cli s3 cp --only-show-errors "\$source" "\$target"
            fi
        }
        export -f nxf_s3_download

        export cache_dir='$cache'
        export cache_state='$cache/state'

        mkdir -p \$cache_dir || true
        touch \$cache_state || true

        search_cache() {
            s3path=\$1
            cache_key=\$( awk -v key="\$s3path" '\$0==key {print NR}' \$cache_state )
            echo \$cache_key
        }
        export -f search_cache

        add_cache_key() {
            s3path=\$1
            #cache_key=\$( awk -v key=\$s3path 'BEGIN{FS=","} \$1==key {print \$2}' cache_state )
            cache_key=\$( search_cache "\$s3path" )

            if [[ -z "\${cache_key}" ]];
                then
                    #echo key not found
                    echo "\$s3path" >> \$cache_state
                #else
                    #echo key found: \$cache_key
            fi

            cache_key=\$( search_cache "\$s3path" )

            echo \$cache_key
        }
        export -f add_cache_key

        stage_object() {
            s3path=\$1
            cache_object=\$2
            if [ ! -e \$cache_object ]; then
                nxf_s3_download "\$s3path" \$cache_object
            fi
        }
        export -f stage_object

        cache_object() {
            s3path=\$1
            object_link=\$2
            state_lock=\${cache_state}.lock
            cache_key=\$( flock \${state_lock} bash -c "add_cache_key \$s3path" )
            object_lock=\${cache_dir}/\${cache_key}.lock
            cache_object=\${cache_dir}/\${cache_key}
            flock \${object_lock} bash -c "stage_object \$s3path \$cache_object"
            if [ -z "\$object_link" ]; then
                object_link=\$( basename "\$s3path" )
            fi
            ln -s \$cache_object "\$object_link"
        }

        """.stripIndent(true)
    }

    /**
     * Implement S3 upload/download helper using s3cmd CLI tool
     * https://github.com/peak/s5cmd
     *
     * @return The Bash script implementing the S3 helper functions
     */
    protected String s5cmdLib() {
        final cli = s5cmdPath
        """
        # aws helper for s5cmd
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              local tmp=\$(nxf_mktemp)
              cp /dev/stdin \$tmp/\$name
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass \$tmp/\$name "\$s3path"
            elif [[ -d "\$name" ]]; then
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name/" "\$s3path/\$name/"
            else
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name" "\$s3path/\$name"
            fi
        }
        
        nxf_s3_download() {
            local source=\$1
            local target=\$2
            local file_name=\$(basename \$1)
            local is_dir=\$($cli ls \$source | grep -F "DIR  \${file_name}/" -c)
            if [[ \$is_dir == 1 ]]; then
                $cli cp "\$source/*" "\$target"
            else 
                $cli cp "\$source" "\$target"
            fi
        }
        """.stripIndent()
    }

    @Override
    String render() {
        return s5cmdPath
                ? super.render() + s5cmdLib()
                : super.render() + retryEnv() + s3Lib()
    }

    static private S3BashLib lib0(AwsOptions opts, boolean includeCore) {
        new S3BashLib()
                .includeCoreFun(includeCore)
                .withMaxParallelTransfers( opts.maxParallelTransfers )
                .withDelayBetweenAttempts(opts.delayBetweenAttempts )
                .withMaxTransferAttempts( opts.maxTransferAttempts )
                .withCliPath( opts.awsCli )
                .withStorageClass(opts.storageClass )
                .withStorageEncryption( opts.storageEncryption )
                .withStorageKmsKeyId( opts.storageKmsKeyId )
                .withRetryMode( opts.retryMode )
                .withDebug( opts.debug )
                .withS5cmdPath( opts.s5cmdPath )
                .withAcl( opts.s3Acl )
                .withRequesterPays( opts.requesterPays )
                .withCache( opts.cache )
    }

    static String script(AwsOptions opts) {
        lib0(opts,true).render()
    }

    static String script() {
        final opts = new AwsOptions(Global.session as Session)
        lib0(opts,false).render()
    }
}
