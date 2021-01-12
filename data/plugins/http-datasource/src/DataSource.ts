import _ from 'lodash';
import defaults from 'lodash/defaults';
import { Base64 } from 'js-base64';

import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';

import { getBackendSrv } from '@grafana/runtime';
import { MyQuery, MyDataSourceOptions, defaultQuery } from './types';

export class DataSource extends DataSourceApi<MyQuery, MyDataSourceOptions> {
  baseUrl: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);

    this.baseUrl = instanceSettings.url!;
  }

  async query(options: DataQueryRequest<MyQuery>): Promise<DataQueryResponse> {
    const promises = options.targets.map(async target => {
      const query = defaults(target, defaultQuery);
      const response = await this.request('', `query=${query.queryText}`);

      /**
       * In this example, the /api/metrics endpoint returns:
       *
       * {
       *   "datapoints": [
       *     {
       *       Time: 1234567891011,
       *       Value: 12.5
       *     },
       *     {
       *     ...
       *   ]
       * }
       */
      console.log('stevensli1', response);
      console.log('stevensli2', response.data);
      console.log('stevensli3', response.data.Response);
      console.log('stevensli4', response.data.Response.Result);

      const resu = response.data.Response.Result;
      console.log('stevensli5', 'resu', resu);
      let metricDataList = Base64.decode(resu);
      console.log('stevensli6', 'metricData', metricDataList);

      //const datapoints = response.data.datapoints;

      //const resu = Base64.decode(response.data.Response.Result);
      //console.log('stevensli', resu);

      Base64.encode('apple');

      metricDataList = JSON.parse(metricDataList);
      let metricData: any;
      metricData = metricDataList[0];
      //metricData = JSON.parse(metricData);
      console.log('stevensli7', metricData);
      let first = metricData.First;
      console.log('stevensli8', first);
      const last = metricData.Last;
      const interval = metricData.Interval;
      let dps = metricData.Dps;
      console.log('stevensli9', dps);
      //dps = JSON.parse(dps);

      const timestamps: number[] = [];
      const values: number[] = [];

      //var stringTime = '2014-07-10 10:21:12';
      //var timestamp2 = Date.parse(new Date(stringTime));
      //timestamp2 = timestamp2 / 1000;

      //for (let i = 0; i < datapoints.length; i++) {
      //timestamps.push(datapoints[i].Time);
      //values.push(datapoints[i].Value);
      //}
      const gap = last - first;
      for (let i = 0; i * interval < gap; i++) {
        const timeSlice = first + i * interval;
        //console.log('stevensli10', timeSlice);
        timestamps.push(timeSlice * 1000);
        values.push(dps[i]);
      }

      return new MutableDataFrame({
        refId: query.refId,
        fields: [
          { name: 'Time', type: FieldType.time, values: timestamps },
          { name: 'Value', type: FieldType.number, values: values },
        ],
      });
    });

    return Promise.all(promises).then(data => ({ data }));
  }

  async request(url: string, params?: string) {
    return getBackendSrv().datasourceRequest({
      method: 'POST',
      url: `${this.baseUrl}${url}`,
      data:
        '{"Downsample":"30s-max","End":1610458826,"Host":"","Metric":"EMR.19725.HDFS.NN.FILES.TOTAL","Start":1610451626,"Action":"DescribeEmrMetricData","RequestId":"8427701c-af4f-47b4-9af7-f9b0b6b7c436","AppId":1258469122,"Uin":"100008965662","SubAccountUin":"100006124200","ClientIp":"9.139.12.121","ApiModule":"emr","Region":"ap-chongqing","Token":"192799577fb624c19a7e04a6799d07dd099688c110001","Version":"2019-01-03","RequestSource":"MC","Language":"zh-CN","Timestamp":"1610444369"}',
    });
  }

  /**
   * Checks whether we can connect to the API.
   */
  async testDatasource() {
    const defaultErrorMessage = 'Cannot connect to API';

    try {
      const response = await this.request('/healthz');
      if (response.status === 200) {
        return {
          status: 'success',
          message: 'Success',
        };
      } else {
        return {
          status: 'error',
          message: response.statusText ? response.statusText : defaultErrorMessage,
        };
      }
    } catch (err) {
      if (_.isString(err)) {
        return {
          status: 'error',
          message: err,
        };
      } else {
        let message = '';
        message += err.statusText ? err.statusText : defaultErrorMessage;
        if (err.data && err.data.error && err.data.error.code) {
          message += ': ' + err.data.error.code + '. ' + err.data.error.message;
        }

        return {
          status: 'error',
          message,
        };
      }
    }
  }
}
