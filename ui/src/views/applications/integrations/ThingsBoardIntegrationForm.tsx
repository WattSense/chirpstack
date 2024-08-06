import { Form, Input, Button, Typography } from "antd";

import { ThingsBoardIntegration } from "@chirpstack/chirpstack-api-grpc-web/api/application_pb";

import { onFinishFailed } from "../../helpers";

interface IProps {
  initialValues: ThingsBoardIntegration;
  onFinish: (obj: ThingsBoardIntegration) => void;
}

function ThingsBoardIntegrationForm(props: IProps) {
  const onFinish = (values: ThingsBoardIntegration.AsObject) => {
    const v = Object.assign(props.initialValues.toObject(), values);
    const i = new ThingsBoardIntegration();

    i.setApplicationId(v.applicationId);
    i.setServer(v.server);

    props.onFinish(i);
  };

  return (
    <Form
      layout="vertical"
      initialValues={props.initialValues.toObject()}
      onFinish={onFinish}
      onFinishFailed={onFinishFailed}
    >
      <Form.Item
        label="ThingsBoard server"
        name="server"
        rules={[
          {
            required: true,
            message: "Please enter the address to the ThingsBoard server!",
          },
        ]}
      >
        <Input placeholder="http://host:port" />
      </Form.Item>
      <Form.Item>
        <Typography.Paragraph>
          Each device must have a 'ThingsBoardAccessToken' variable assigned. This access-token is generated by
          ThingsBoard.
        </Typography.Paragraph>
      </Form.Item>
      <Form.Item>
        <Button type="primary" htmlType="submit">
          Submit
        </Button>
      </Form.Item>
    </Form>
  );
}

export default ThingsBoardIntegrationForm;
