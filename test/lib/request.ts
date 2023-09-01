interface Args {
  url: string;
  body?: string | object;
  form?: string | object;
  type?: string;
  method?: string;
  headers?: object;
}

interface ContentType {
  'Content-Type'?: string;
}

export default async function request(args: string | Args): Promise<any> {
  let headerOptions;
  let bodyOptions;
  let formOptions;
  let response: Response;
  let url: string;
  let method: string = 'GET';
  let type: string = 'text';
  let contentType: ContentType = {};

  try {
    if (typeof args === 'string') {
      url = args;
      response = await fetch(args);
    } else {
      ({
        url,
        method = 'GET',
        type = 'text',
        headers: headerOptions,
        body: bodyOptions,
        form: formOptions,
      } = args);
      const requestObject: any = { method };

      if (bodyOptions) {
        switch (typeof bodyOptions) {
          case 'object':
            bodyOptions = JSON.stringify(bodyOptions);
            break;
          case 'string':
            break;
          default:
            throw new Error('Check to body type(string, object)');
        }
        const body = bodyOptions;
        contentType = { 'Content-Type': 'application/json' };
        requestObject.body = body;
      } else if (formOptions) {
        switch (typeof formOptions) {
          case 'object':
            formOptions = new URLSearchParams(Object.entries(formOptions)).toString();
            break;
          case 'string':
            break;
          default:
            throw new Error('Check to form type(string, object)');
        }
        const form = formOptions;
        contentType = { 'Content-Type': 'application/x-www-form-urlencoded' };
        requestObject.body = form;
      }

      if (Object.keys(contentType).length || headerOptions) {
        headerOptions = Object.entries({
          ...contentType,
          ...headerOptions,
        }).map(([key, value]) => [key, String(value)]);
        const headers = headerOptions;
        requestObject.headers = headers;
      }
      response = await fetch(url, requestObject);
    }

    if (!response.ok) {
      throw new Error(`response error ${response.status} ${response.statusText}`);
    }

    switch (type) {
      case 'blob':
        return response.blob();
      case 'buffer':
        return Buffer.from(await response.arrayBuffer());
      case 'text':
        return response.text();
      case 'json':
        return response.json();
      default:
        return response;
    }
  } catch (e) {
    console.error(`fetch error(url: ${url}) :${e}`);
    throw new Error(`error(url: '${url}' ): ${e}`);
  }
}
